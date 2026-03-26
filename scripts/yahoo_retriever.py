#!/usr/bin/env python3
"""Standalone Yahoo Finance stock fundamentals retriever.

Scrapes company lists (NASDAQ OMX Stockholm and/or S&P 500), fetches
financial fundamentals via yahooquery, computes Piotroski scores, and
writes everything into local Delta tables.

Usage:
    python scripts/yahoo_retriever.py

Environment variables:
    YAHOO_MARKETS          Comma-separated markets (default: "nasdaq_omx,sp500")
    YAHOO_OUTPUT_DIR       Root Delta table path (default: data/yahoo/delta)
    YAHOO_MAX_TICKERS      Max tickers per market, 0=all (default: 0)
"""

import logging
import os
import time
from datetime import date

import pandas as pd
import pyarrow as pa
import requests
from yahooquery import Ticker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Company list scrapers
# ---------------------------------------------------------------------------

def scrape_nasdaq_omx_companies() -> pd.DataFrame:
    """Get NASDAQ OMX Stockholm listed companies.

    The nasdaqomxnordic.com site now redirects to nasdaq.com and no longer
    serves a scrapeable HTML table.  We use the Nasdaq Nordic instruments
    JSON API instead (undocumented, but stable as of 2025).
    Falls back to a curated Large/Mid-Cap list if the API is unavailable.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/81.0.4044.141 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    # Try the Nasdaq Nordic instruments API
    api_url = (
        "https://api.nasdaq.com/api/nordic/instruments"
        "?exchange=STO&type=SHARES&limit=2000"
    )
    try:
        resp = requests.get(api_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            rows = data.get("data", {}).get("table", {}).get("rows")
            if rows:
                df = pd.DataFrame(rows)
                df["ticker"] = df["symbol"].str.replace(" ", "-") + ".ST"
                df["market"] = "nasdaq_omx"
                log.info("Fetched %d companies from Nasdaq Nordic API", len(df))
                return df
    except Exception as exc:
        log.warning("Nasdaq Nordic API failed: %s — using fallback list", exc)

    # Fallback: curated list of major Stockholm-listed companies
    log.info("Using fallback curated Stockholm company list")
    return _fallback_stockholm_companies()


# Large/Mid-Cap Stockholm tickers (Yahoo Finance format).
# Update this list periodically if the API fallback is needed.
_STOCKHOLM_TICKERS = [
    ("Volvo B", "VOLV-B.ST"), ("Atlas Copco A", "ATCO-A.ST"),
    ("Ericsson B", "ERIC-B.ST"), ("Investor B", "INVE-B.ST"),
    ("ABB", "ABB.ST"), ("Hexagon B", "HEXA-B.ST"),
    ("Sandvik", "SAND.ST"), ("SEB A", "SEB-A.ST"),
    ("Swedbank A", "SWED-A.ST"), ("Handelsbanken A", "SHB-A.ST"),
    ("H&M B", "HM-B.ST"), ("Essity B", "ESSITY-B.ST"),
    ("Assa Abloy B", "ASSA-B.ST"), ("Alfa Laval", "ALFA.ST"),
    ("SKF B", "SKF-B.ST"), ("Epiroc A", "EPI-A.ST"),
    ("Telia", "TELIA.ST"), ("Boliden", "BOL.ST"),
    ("Electrolux B", "ELUX-B.ST"), ("Getinge B", "GETI-B.ST"),
    ("Nibe B", "NIBE-B.ST"), ("Husqvarna B", "HUSQ-B.ST"),
    ("Sinch", "SINCH.ST"), ("Kinnevik B", "KINV-B.ST"),
    ("Securitas B", "SECU-B.ST"), ("Tele2 B", "TEL2-B.ST"),
    ("Swedish Match", "SWMA.ST"), ("Skanska B", "SKA-B.ST"),
    ("Latour B", "LATO-B.ST"), ("Saab B", "SAAB-B.ST"),
    ("Trelleborg B", "TREL-B.ST"), ("Lundbergforetagen B", "LUND-B.ST"),
    ("Lifco B", "LIFCO-B.ST"), ("Indutrade", "INDT.ST"),
    ("Addtech B", "ADDT-B.ST"), ("Lagercrantz B", "LAGR-B.ST"),
    ("Sweco B", "SWEC-B.ST"), ("Hexpol B", "HPOL-B.ST"),
    ("Vitrolife", "VITR.ST"), ("BillerudKorsnas", "BILL.ST"),
]


def _fallback_stockholm_companies() -> pd.DataFrame:
    """Return a curated DataFrame of major Stockholm-listed companies."""
    df = pd.DataFrame(_STOCKHOLM_TICKERS, columns=["name", "ticker"])
    df["market"] = "nasdaq_omx"
    return df


def scrape_sp500_companies() -> pd.DataFrame:
    """Scrape the S&P 500 company list from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/81.0.4044.141 Safari/537.36"
        )
    }
    from io import StringIO

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(StringIO(resp.text))
    df = tables[0]
    df = df.rename(columns={"Symbol": "ticker"})
    df["market"] = "sp500"
    return df


MARKET_SCRAPERS = {
    "nasdaq_omx": scrape_nasdaq_omx_companies,
    "sp500": scrape_sp500_companies,
}


# ---------------------------------------------------------------------------
# Financial data fetchers
# ---------------------------------------------------------------------------

class StockFundamentals:
    """Fetch fundamental data for a single ticker via yahooquery."""

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.ticker = Ticker(symbol)

    def fetch(self) -> dict[str, pd.DataFrame]:
        """Return a dict of dataset_name -> DataFrame."""
        datasets = {}

        income = self.ticker.income_statement(frequency="q", trailing=False)
        if isinstance(income, pd.DataFrame) and not income.empty:
            income = income.sort_values("asOfDate").set_index("asOfDate")
            income["Ticker"] = self.symbol
            datasets["income_statement"] = income

        balance = self.ticker.balance_sheet(frequency="q")
        if isinstance(balance, pd.DataFrame) and not balance.empty:
            balance = balance.sort_values("asOfDate").set_index("asOfDate")
            balance["Ticker"] = self.symbol
            datasets["balance_sheet"] = balance

        cashflow = self.ticker.cash_flow(frequency="q", trailing=False)
        if isinstance(cashflow, pd.DataFrame) and not cashflow.empty:
            cashflow = cashflow.sort_values("asOfDate").set_index("asOfDate")
            cashflow["Ticker"] = self.symbol
            datasets["cash_flow"] = cashflow

        valuation = self.ticker.valuation_measures
        if isinstance(valuation, pd.DataFrame) and not valuation.empty:
            if "EnterpriseValue" in valuation.columns:
                valuation = valuation[~valuation.EnterpriseValue.isnull()]
            datasets["valuation_measure"] = valuation

        asset_profile = self.ticker.asset_profile
        if isinstance(asset_profile, dict):
            try:
                ap = pd.DataFrame(asset_profile).T
                if "companyOfficers" in ap.columns:
                    ap = ap.drop(columns=["companyOfficers"])
                datasets["asset_profile"] = ap
            except Exception:
                pass

        financial_data = self.ticker.financial_data
        if isinstance(financial_data, dict):
            try:
                datasets["financial_data"] = pd.DataFrame(financial_data).T
            except Exception:
                pass

        return datasets


# ---------------------------------------------------------------------------
# Piotroski F-Score
# ---------------------------------------------------------------------------

def compute_piotroski_score(
    income: pd.DataFrame,
    balance: pd.DataFrame,
    cashflow: pd.DataFrame,
    ticker: str,
) -> pd.DataFrame | None:
    """Compute Piotroski F-Score across available quarters."""
    if any(df is None or len(df) < 2 for df in [income, balance, cashflow]):
        return None

    score_rows = []
    for i in range(1, len(balance.index)):
        dt = balance.index[i]
        prev = balance.index[i - 1]

        try:
            # Profitability
            roa = income.loc[dt, "NetIncome"] / balance.loc[dt, "TotalAssets"]
            prev_roa = income.loc[prev, "NetIncome"] / balance.loc[prev, "TotalAssets"]
            cfo = cashflow.loc[dt, "OperatingCashFlow"]

            roa_score = int(roa > 0)
            cfo_score = int(cfo > 0)
            delta_roa_score = int(roa - prev_roa > 0)
            quality_score = int(cfo > income.loc[dt, "NetIncome"])

            # Leverage / liquidity
            leverage = balance.loc[dt, "LongTermDebt"] / balance.loc[dt, "TotalAssets"]
            prev_leverage = balance.loc[prev, "LongTermDebt"] / balance.loc[prev, "TotalAssets"]
            delta_leverage_score = int(leverage - prev_leverage < 0)

            liquidity = balance.loc[dt, "CurrentAssets"] / balance.loc[dt, "CurrentLiabilities"]
            prev_liquidity = balance.loc[prev, "CurrentAssets"] / balance.loc[prev, "CurrentLiabilities"]
            delta_liquidity_score = int(liquidity - prev_liquidity > 0)

            new_equity = balance.loc[dt, "CommonStock"] - balance.loc[prev, "CommonStock"]
            new_equity_score = int(new_equity <= 0)

            # Operating efficiency
            gm = (income.loc[dt, "TotalRevenue"] - income.loc[dt, "CostOfRevenue"]) / income.loc[dt, "TotalRevenue"]
            prev_gm = (income.loc[prev, "TotalRevenue"] - income.loc[prev, "CostOfRevenue"]) / income.loc[prev, "TotalRevenue"]
            gross_margin_score = int(gm > prev_gm)

            at = income.loc[dt, "TotalRevenue"] / balance.loc[dt, "TotalAssets"]
            prev_at = income.loc[prev, "TotalRevenue"] / balance.loc[prev, "TotalAssets"]
            asset_turnover_score = int(at > prev_at)

            total = sum([
                roa_score, cfo_score, delta_roa_score, quality_score,
                delta_leverage_score, delta_liquidity_score, new_equity_score,
                gross_margin_score, asset_turnover_score,
            ])

            score_rows.append({
                "Date": dt,
                "Ticker": ticker,
                "ROA": roa_score,
                "CFO": cfo_score,
                "DeltaROA": delta_roa_score,
                "QualityOfEarnings": quality_score,
                "DeltaLeverage": delta_leverage_score,
                "DeltaLiquidity": delta_liquidity_score,
                "NewEquity": new_equity_score,
                "GrossMargin": gross_margin_score,
                "AssetTurnover": asset_turnover_score,
                "PiotroskiScore": total,
            })
        except (KeyError, ZeroDivisionError, TypeError):
            continue

    if not score_rows:
        return None
    return pd.DataFrame(score_rows)


# ---------------------------------------------------------------------------
# Delta writer
# ---------------------------------------------------------------------------

def _has_null_type(t: pa.DataType) -> bool:
    """Check whether a PyArrow type is or contains the null type."""
    if pa.types.is_null(t):
        return True
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return _has_null_type(t.value_type)
    if pa.types.is_struct(t):
        return any(_has_null_type(t.field(i).type) for i in range(t.num_fields))
    return False


def _sanitize_arrow_table(table: pa.Table) -> pa.Table:
    """Replace null-typed or problematic columns with string so Delta can handle them."""
    new_fields = []
    new_columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if _has_null_type(field.type):
            new_fields.append(pa.field(field.name, pa.string(), nullable=True))
            new_columns.append(pa.array([None] * len(col), type=pa.string()))
        else:
            new_fields.append(field)
            new_columns.append(col)
    return pa.table(new_columns, schema=pa.schema(new_fields))


def write_delta(df: pd.DataFrame, table_path: str, merge_key: str | None = None) -> None:
    """Write or merge a DataFrame into a Delta table."""
    from deltalake import DeltaTable, write_deltalake

    table = _sanitize_arrow_table(pa.Table.from_pandas(df, preserve_index=False))
    os.makedirs(table_path, exist_ok=True)

    if not DeltaTable.is_deltatable(table_path):
        write_deltalake(table_path, table)
        log.info("Created Delta table at %s (%d rows)", table_path, len(df))
        return

    if merge_key:
        dt = DeltaTable(table_path)
        (
            dt.merge(
                source=table,
                predicate=f"target.{merge_key} = source.{merge_key}",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        log.info("Merged %d rows into %s", len(df), table_path)
    else:
        write_deltalake(
            table_path, table, mode="overwrite", schema_mode="overwrite",
        )
        log.info("Overwrote Delta table at %s (%d rows)", table_path, len(df))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def fetch_ticker_fundamentals(ticker: str, ingest_date: str) -> dict[str, pd.DataFrame]:
    """Fetch all fundamentals for a single ticker.

    Returns a dict of dataset_name -> DataFrame (all with ingest_date and
    reset index).  Piotroski score is computed and included if possible.
    """
    stock = StockFundamentals(ticker)
    datasets = stock.fetch()

    if not datasets:
        log.warning("  %s — no data returned, skipping", ticker)
        return {}

    result: dict[str, pd.DataFrame] = {}
    for name, df in datasets.items():
        df = df.reset_index()
        df["ingest_date"] = ingest_date
        result[name] = df

    # Compute Piotroski score
    income = datasets.get("income_statement")
    balance = datasets.get("balance_sheet")
    cashflow = datasets.get("cash_flow")

    if income is not None and balance is not None and cashflow is not None:
        scores = compute_piotroski_score(income, balance, cashflow, ticker)
        if scores is not None:
            scores["ingest_date"] = ingest_date
            result["piotroski_score"] = scores

    return result


def main() -> None:
    from deltalake import DeltaTable

    markets = [
        m.strip()
        for m in os.environ.get("YAHOO_MARKETS", "nasdaq_omx,sp500").split(",")
    ]
    output_dir = os.environ.get("YAHOO_OUTPUT_DIR", "data/yahoo/delta")
    max_tickers = int(os.environ.get("YAHOO_MAX_TICKERS", "0"))
    ingest_date = date.today().strftime("%Y-%m-%d")

    log.info("Starting Yahoo retriever — markets=%s  output=%s", markets, output_dir)

    for market in markets:
        scraper = MARKET_SCRAPERS.get(market)
        if not scraper:
            log.error("Unknown market: %s (available: %s)", market, list(MARKET_SCRAPERS.keys()))
            continue

        log.info("Scraping company list for %s", market)
        companies = scraper()
        log.info("Found %d companies in %s", len(companies), market)

        # Write companies list to Delta
        companies["ingest_date"] = ingest_date
        companies_path = os.path.join(output_dir, "companies", market)
        write_delta(companies, companies_path)

        # Determine ticker column and limit
        tickers = companies["ticker"].tolist()
        if max_tickers > 0:
            tickers = tickers[:max_tickers]
            log.info("Limited to %d tickers", max_tickers)

        # Fetch fundamentals for each ticker, batching results in memory
        batched: dict[str, list[pd.DataFrame]] = {}
        failed = 0
        for i, ticker in enumerate(tickers, 1):
            log.info("[%d/%d] Fetching %s (%s)", i, len(tickers), ticker, market)
            try:
                ticker_data = fetch_ticker_fundamentals(ticker, ingest_date)
                for name, df in ticker_data.items():
                    batched.setdefault(name, []).append(df)
            except Exception:
                log.exception("  Failed to process %s", ticker)
                failed += 1
            time.sleep(0.5)  # rate-limit

        # Write each dataset once per market (single parquet file per dataset)
        for name, dfs in batched.items():
            combined = pd.concat(dfs, ignore_index=True)
            table_path = os.path.join(output_dir, name, market)
            write_delta(combined, table_path)
            log.info("Wrote %s — %d rows, %d tickers", name, len(combined),
                     combined["Ticker"].nunique() if "Ticker" in combined.columns else "?")

        # Vacuum old parquet files for all tables in this market
        for name in [*batched, "companies"]:
            table_path = os.path.join(output_dir, name, market)
            if DeltaTable.is_deltatable(table_path):
                dt = DeltaTable(table_path)
                removed = dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
                if removed:
                    log.info("Vacuumed %s — removed %d old files", name, len(removed))

        log.info(
            "%s complete — %d/%d tickers processed (%d failed)",
            market, len(tickers) - failed, len(tickers), failed,
        )

    log.info("Done.")


if __name__ == "__main__":
    main()
