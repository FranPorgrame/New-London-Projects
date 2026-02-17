import requests
from bs4 import BeautifulSoup
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

def get_mergers_and_acquisitions():
    """
    Recopila información sobre fusiones y adquisiciones de la semana actual
    desde múltiples fuentes públicas.
    """
    results = []

    # ─────────────────────────────────────────────
    # FUENTE 1: Reuters M&A News (RSS Feed)
    # ─────────────────────────────────────────────
    def fetch_reuters_ma():
        url = "https://feeds.reuters.com/reuters/mergersNews"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, "xml")
            items = soup.find_all("item")[:10]

            for item in items:
                pub_date = item.find("pubDate")
                date_str = pub_date.text if pub_date else ""

                # Filtrar solo noticias de los últimos 7 días
                try:
                    pub_dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                    if (datetime.now(pub_dt.tzinfo) - pub_dt).days <= 7:
                        results.append({
                            "fuente": "Reuters",
                            "titulo": item.find("title").text if item.find("title") else "N/A",
                            "descripcion": item.find("description").text[:200] if item.find("description") else "N/A",
                            "url": item.find("link").text if item.find("link") else "N/A",
                            "fecha": date_str
                        })
                except Exception:
                    pass
        except Exception as e:
            print(f"[Reuters] Error: {e}")

    # ─────────────────────────────────────────────
    # FUENTE 2: MarketWatch M&A (RSS Feed)
    # ─────────────────────────────────────────────
    def fetch_marketwatch_ma():
        url = "https://feeds.marketwatch.com/marketwatch/realtimeheadlines/"
        headers = {"User-Agent": "Mozilla/5.0"}
        keywords = ["merger", "acquisition", "acquires", "merges", "takeover", "buyout", "fusión", "adquisición"]
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, "xml")
            items = soup.find_all("item")

            for item in items:
                title = item.find("title").text if item.find("title") else ""
                if any(kw.lower() in title.lower() for kw in keywords):
                    pub_date = item.find("pubDate")
                    date_str = pub_date.text if pub_date else ""
                    try:
                        pub_dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                        if (datetime.now(pub_dt.tzinfo) - pub_dt).days <= 7:
                            results.append({
                                "fuente": "MarketWatch",
                                "titulo": title,
                                "descripcion": item.find("description").text[:200] if item.find("description") else "N/A",
                                "url": item.find("link").text if item.find("link") else "N/A",
                                "fecha": date_str
                            })
                    except Exception:
                        pass
        except Exception as e:
            print(f"[MarketWatch] Error: {e}")

    # ─────────────────────────────────────────────
    # FUENTE 3: Yahoo Finance - Datos de empresa
    # con yfinance (enriquecer resultados con datos
    # financieros de los tickers involucrados)
    # ─────────────────────────────────────────────
    def enrich_with_yfinance(ticker_symbol: str) -> dict:
        """
        Dado un ticker, obtiene información financiera relevante
        para contextualizar una fusión/adquisición.
        """
        try:
            ticker = yf.Ticker(ticker_symbol)
            info = ticker.info
            return {
                "ticker": ticker_symbol,
                "nombre": info.get("longName", "N/A"),
                "sector": info.get("sector", "N/A"),
                "industria": info.get("industry", "N/A"),
                "market_cap": info.get("marketCap", "N/A"),
                "precio_actual": info.get("currentPrice", "N/A"),
                "moneda": info.get("currency", "USD"),
                "pais": info.get("country", "N/A"),
            }
        except Exception as e:
            return {"error": str(e)}

    # ─────────────────────────────────────────────
    # FUENTE 4: Scraping de PR Newswire (M&A section)
    # ─────────────────────────────────────────────
    def fetch_prnewswire_ma():
        url = "https://www.prnewswire.com/rss/news-releases-list.rss?category=mergers-and-acquisitions"
        headers = {"User-Agent": "Mozilla/5.0"}
        try:
            response = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, "xml")
            items = soup.find_all("item")[:10]

            for item in items:
                pub_date = item.find("pubDate")
                date_str = pub_date.text if pub_date else ""
                try:
                    pub_dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
                    if (datetime.now(pub_dt.tzinfo) - pub_dt).days <= 7:
                        results.append({
                            "fuente": "PR Newswire",
                            "titulo": item.find("title").text if item.find("title") else "N/A",
                            "descripcion": item.find("description").text[:200] if item.find("description") else "N/A",
                            "url": item.find("link").text if item.find("link") else "N/A",
                            "fecha": date_str
                        })
                except Exception:
                    pass
        except Exception as e:
            print(f"[PR Newswire] Error: {e}")

    # ─── Ejecutar todas las fuentes ───
    print("🔍 Buscando fusiones y adquisiciones de la semana...")
    fetch_reuters_ma()
    fetch_marketwatch_ma()
    fetch_prnewswire_ma()

    # ─── Formatear y retornar resultados ───
    if not results:
        return "⚠️ No se encontraron fusiones y adquisiciones esta semana."

    df = pd.DataFrame(results).drop_duplicates(subset=["titulo"])
    df = df.sort_values("fecha", ascending=False).reset_index(drop=True)

    print(f"\n✅ {len(df)} resultados encontrados:\n")
    for i, row in df.iterrows():
        print(f"[{i+1}] 📰 {row['fuente']} | {row['fecha']}")
        print(f"     📌 {row['titulo']}")
        print(f"     🔗 {row['url']}\n")

    return df


# ─── Uso del bot ───
if __name__ == "__main__":
    df_ma = get_mergers_and_acquisitions()

    # Ejemplo: enriquecer con datos de yfinance si conocemos el ticker
    # enrich_with_yfinance("AAPL")