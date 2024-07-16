from datetime import timedelta
import numpy as np
import pandas as pd
from psycopg2 import sql
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LinearRegression

from database_operations import get_db_connection

def get_latest_transactions(company_name, limit=5):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT p.id AS transaction_id, c.alias AS "Şirket", p.process_type AS "processType", 
                   p.unit_price AS "unitPrice", p.share_count AS "shareCount", p.listing_at AS "listingAt"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE c.alias ILIKE %s
            ORDER BY p.listing_at DESC
            LIMIT %s
        """, (f"%{company_name}%", limit))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()





def get_new_transactions(last_checked_id):
    conn = get_db_connection()
    cur = conn.cursor()

    last_checked_id = int(last_checked_id)
    
    cur.execute("""
        SELECT p.id AS transaction_id, c.alias AS "Şirket", c.slogan, p.share_count AS "shareCount", 
               p.unit_price AS "unitPrice", (p.share_count * p.unit_price) AS "Toplam Tutar", 
               CASE WHEN p.process_type = 'sell' THEN 'buy' ELSE 'sell' END AS "processType",
               p.listing_at AS "listingAt",
               sg.letter AS "share_group_letter", sg.isin AS "share_group_isin",
               p.processed_share_count AS "processedShareCount", p.open_to_offer AS "openToOffer",
               c.id AS company_id, c.avatar
        FROM placards p
        JOIN companies c ON p.company_id = c.id
        JOIN share_groups sg ON p.share_group_id = sg.id
        WHERE p.id > %s
        ORDER BY p.id ASC
    """, (last_checked_id,))
    
    columns = [desc[0] for desc in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=columns)

    cur.close()
    conn.close()

    return df






def get_all_companies():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("SELECT DISTINCT alias FROM companies ORDER BY alias")
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

def get_latest_transaction_id():
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("SELECT MAX(id) FROM placards")
        return cur.fetchone()[0] or 0
    finally:
        cur.close()
        conn.close()

def get_company_stats(company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                c.alias AS "Şirket",
                COUNT(*) AS "İşlem Sayısı",
                AVG(p.unit_price) AS "Ortalama Fiyat",
                SUM(p.share_count) AS "Toplam Pay Adedi",
                MIN(p.listing_at) AS "İlk İşlem Tarihi",
                MAX(p.listing_at) AS "Son İşlem Tarihi"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE c.alias ILIKE %s
            GROUP BY c.alias
        """, (f"%{company_name}%",))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_price_history(company_name, days=30):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                c.alias AS "Şirket",
                p.listing_at AS "Tarih",
                p.unit_price AS "Fiyat"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE c.alias ILIKE %s
              AND p.listing_at >= NOW() - INTERVAL '%s days'
            ORDER BY p.listing_at
        """, (f"%{company_name}%", days))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_top_companies_by_transaction_volume(limit=10, start_date=None, end_date=None):
    """
    İşlem hacmine göre en yüksek şirketleri getirir.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = """
        SELECT c.alias AS "Şirket", 
               COUNT(*) AS "İşlem Sayısı",
               SUM(p.share_count * p.unit_price) AS "Toplam Hacim"
        FROM placards p
        JOIN companies c ON p.company_id = c.id
        WHERE 1=1
    """
    
    params = []
    if start_date:
        query += " AND p.listing_at >= %s"
        params.append(start_date)
    if end_date:
        query += " AND p.listing_at <= %s"
        params.append(end_date)
    
    query += """
        GROUP BY c.alias
        ORDER BY "Toplam Hacim" DESC
        LIMIT %s
    """
    params.append(limit)
    
    try:
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_price_trend(company_name, interval='daily'):
    """
    Belirli bir şirket için fiyat trendini getirir.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    interval_sql = "DATE_TRUNC(%s, p.listing_at)"
    if interval == 'weekly':
        interval_sql = "DATE_TRUNC('week', p.listing_at)"
    elif interval == 'monthly':
        interval_sql = "DATE_TRUNC('month', p.listing_at)"
    
    try:
        cur.execute(f"""
            SELECT 
                {interval_sql} AS "Tarih",
                AVG(p.unit_price) AS "Ortalama Fiyat",
                MIN(p.unit_price) AS "En Düşük Fiyat",
                MAX(p.unit_price) AS "En Yüksek Fiyat"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE c.alias ILIKE %s
            GROUP BY {interval_sql}
            ORDER BY {interval_sql}
        """, (interval, f"%{company_name}%"))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_share_group_distribution(company_name):
    """
    Belirli bir şirket için pay grubu dağılımını getirir.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                sg.letter AS "Pay Grubu",
                COUNT(*) AS "İşlem Sayısı",
                SUM(p.share_count) AS "Toplam Pay Adedi",
                AVG(p.unit_price) AS "Ortalama Fiyat"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            JOIN share_groups sg ON p.share_group_id = sg.id
            WHERE c.alias ILIKE %s
            GROUP BY sg.letter
            ORDER BY "Toplam Pay Adedi" DESC
        """, (f"%{company_name}%",))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_company_comparison(company_names):
    """
    Birden fazla şirketin karşılaştırmalı analizini yapar.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    placeholders = ', '.join(['%s'] * len(company_names))
    
    try:
        cur.execute(f"""
            SELECT 
                c.alias AS "Şirket",
                COUNT(*) AS "Toplam İşlem Sayısı",
                AVG(p.unit_price) AS "Ortalama Fiyat",
                SUM(p.share_count) AS "Toplam Pay Adedi",
                SUM(p.share_count * p.unit_price) AS "Toplam İşlem Hacmi",
                MIN(p.listing_at) AS "İlk İşlem Tarihi",
                MAX(p.listing_at) AS "Son İşlem Tarihi"
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE c.alias IN ({placeholders})
            GROUP BY c.alias
            ORDER BY "Toplam İşlem Hacmi" DESC
        """, company_names)
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_market_sentiment(days=30):
    """
    Son 'days' gün içindeki piyasa duyarlılığını analiz eder.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            WITH daily_stats AS (
                SELECT 
                    DATE_TRUNC('day', p.listing_at) AS trade_date,
                    SUM(CASE WHEN p.process_type = 'buy' THEN 1 ELSE 0 END) AS buy_count,
                    SUM(CASE WHEN p.process_type = 'sell' THEN 1 ELSE 0 END) AS sell_count,
                    AVG(p.unit_price) AS avg_price
                FROM placards p
                WHERE p.listing_at >= NOW() - INTERVAL %s DAY
                GROUP BY DATE_TRUNC('day', p.listing_at)
            )
            SELECT 
                trade_date AS "Tarih",
                buy_count AS "Alım Sayısı",
                sell_count AS "Satım Sayısı",
                CASE 
                    WHEN buy_count > sell_count THEN 'Pozitif'
                    WHEN sell_count > buy_count THEN 'Negatif'
                    ELSE 'Nötr'
                END AS "Duyarlılık",
                avg_price AS "Ortalama Fiyat"
            FROM daily_stats
            ORDER BY trade_date
        """, (f"{days} day",))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def get_market_anomalies(threshold=2):
    """
    Piyasadaki anormal hareketleri tespit eder.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            WITH company_stats AS (
                SELECT 
                    c.alias AS company,
                    AVG(p.unit_price) AS avg_price,
                    STDDEV(p.unit_price) AS stddev_price,
                    MAX(p.unit_price) AS max_price,
                    MIN(p.unit_price) AS min_price
                FROM placards p
                JOIN companies c ON p.company_id = c.id
                WHERE p.listing_at >= NOW() - INTERVAL '30 days'
                GROUP BY c.alias
            )
            SELECT 
                company,
                avg_price,
                stddev_price,
                max_price,
                min_price,
                CASE 
                    WHEN (max_price - avg_price) / stddev_price > %s THEN 'Anormal Yükseliş'
                    WHEN (avg_price - min_price) / stddev_price > %s THEN 'Anormal Düşüş'
                    ELSE 'Normal'
                END AS status
            FROM company_stats
            WHERE (max_price - avg_price) / stddev_price > %s OR (avg_price - min_price) / stddev_price > %s
            ORDER BY ABS((max_price - avg_price) / stddev_price) DESC
        """, (threshold, threshold, threshold, threshold))
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()

def predict_price_trends(days=30):
    """
    Gelecek 'days' gün için fiyat trendlerini tahmin eder.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                c.alias AS company,
                p.listing_at AS date,
                p.unit_price AS price
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE p.listing_at >= NOW() - INTERVAL '90 days'
            ORDER BY c.alias, p.listing_at
        """)
        
        df = pd.DataFrame(cur.fetchall(), columns=['company', 'date', 'price'])
        df['date'] = pd.to_datetime(df['date'])
        
        predictions = []
        
        for company in df['company'].unique():
            company_data = df[df['company'] == company]
            X = (company_data['date'] - company_data['date'].min()).dt.days.values.reshape(-1, 1)
            y = company_data['price'].values
            
            model = LinearRegression()
            model.fit(X, y)
            
            future_dates = pd.date_range(start=company_data['date'].max() + timedelta(days=1), periods=days)
            future_X = (future_dates - company_data['date'].min()).days.values.reshape(-1, 1)
            
            future_prices = model.predict(future_X)
            
            for date, price in zip(future_dates, future_prices):
                predictions.append({
                    'company': company,
                    'date': date,
                    'predicted_price': price
                })
        
        return pd.DataFrame(predictions)
    finally:
        cur.close()
        conn.close()

def get_correlated_companies(threshold=0.7):
    """
    Fiyat hareketleri yüksek korelasyona sahip şirketleri bulur.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                c.alias AS company,
                p.listing_at AS date,
                p.unit_price AS price
            FROM placards p
            JOIN companies c ON p.company_id = c.id
            WHERE p.listing_at >= NOW() - INTERVAL '90 days'
            ORDER BY c.alias, p.listing_at
        """)
        
        df = pd.DataFrame(cur.fetchall(), columns=['company', 'date', 'price'])
        df['date'] = pd.to_datetime(df['date'])
        
        pivot_df = df.pivot(index='date', columns='company', values='price')
        correlation_matrix = pivot_df.corr()
        
        high_correlations = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                if abs(correlation_matrix.iloc[i, j]) > threshold:
                    high_correlations.append({
                        'company1': correlation_matrix.columns[i],
                        'company2': correlation_matrix.columns[j],
                        'correlation': correlation_matrix.iloc[i, j]
                    })
        
        return pd.DataFrame(high_correlations)
    finally:
        cur.close()
        conn.close()

def get_market_efficiency():
    """
    Piyasa etkinliğini ölçer (Hurst Exponent kullanarak).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                p.listing_at AS date,
                AVG(p.unit_price) AS avg_price
            FROM placards p
            WHERE p.listing_at >= NOW() - INTERVAL '365 days'
            GROUP BY p.listing_at
            ORDER BY p.listing_at
        """)
        
        df = pd.DataFrame(cur.fetchall(), columns=['date', 'price'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)

        # Eksik veya NaN değerleri kontrol edip kaldırma
        df = df.dropna(subset=['price'])
        
        returns = np.log(df['price'] / df['price'].shift(1)).dropna()
        
        def hurst_exponent(returns):
            lags = range(2, 100)
            tau = [np.sqrt(np.std(np.subtract(returns[lag:], returns[:-lag]))) for lag in lags]
            poly = np.polyfit(np.log(lags), np.log(tau), 1)
            return poly[0] * 2.0
        
        h = hurst_exponent(returns)
        
        interpretation = (
            "Etkin Pazar" if 0.45 <= h <= 0.55 else
            "Trend Takip Eden" if h > 0.55 else
            "Ortalamaya Dönen"
        )
        
        return pd.DataFrame([{'Hurst Exponent': h, 'Yorum': interpretation}])
    finally:
        cur.close()
        conn.close()




def add_subscription(user_id, company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO user_subscriptions (user_id, company_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id, company_name) DO NOTHING
        """, (user_id, company_name))
        conn.commit()
        return True
    except Exception as e:
        print(f"Abonelik eklenirken hata oluştu: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def remove_subscription(user_id, company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if company_name == 'ALL':
            # Tüm abonelikleri kaldır
            cur.execute("""
                DELETE FROM user_subscriptions
                WHERE user_id = %s
            """, (user_id,))
        else:
            cur.execute("""
                DELETE FROM user_subscriptions
                WHERE user_id = %s AND company_name = %s
            """, (user_id, company_name))
        conn.commit()
        return True
    except Exception as e:
        print(f"Abonelik kaldırılırken hata oluştu: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def get_subscribed_users(company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT DISTINCT user_id FROM user_subscriptions
            WHERE company_name = 'ALL' OR company_name = %s
        """, (company_name,))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()





def add_subscription(user_id, company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO user_subscriptions (user_id, company_name)
            VALUES (%s, %s)
            ON CONFLICT (user_id, company_name) DO NOTHING
        """, (user_id, company_name))
        conn.commit()
        return True
    except Exception as e:
        print(f"Abonelik eklenirken hata oluştu: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()


def add_subscription(user_id, company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if company_name is None:
            # Tüm şirketlere abonelik için özel bir kayıt
            cur.execute("""
                INSERT INTO user_subscriptions (user_id, company_name)
                VALUES (%s, 'ALL')
                ON CONFLICT (user_id, company_name) DO NOTHING
            """, (user_id,))
        else:
            cur.execute("""
                INSERT INTO user_subscriptions (user_id, company_name)
                VALUES (%s, %s)
                ON CONFLICT (user_id, company_name) DO NOTHING
            """, (user_id, company_name))
        conn.commit()
        return True
    except Exception as e:
        print(f"Abonelik eklenirken hata oluştu: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def remove_subscription(user_id, company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        if company_name is None:
            # Tüm abonelikleri kaldır
            cur.execute("""
                DELETE FROM user_subscriptions
                WHERE user_id = %s
            """, (user_id,))
        else:
            cur.execute("""
                DELETE FROM user_subscriptions
                WHERE user_id = %s AND company_name = %s
            """, (user_id, company_name))
        conn.commit()
        return True
    except Exception as e:
        print(f"Abonelik kaldırılırken hata oluştu: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

def get_subscribed_users(company_name):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT DISTINCT user_id FROM user_subscriptions
            WHERE company_name = 'ALL' OR company_name = %s
        """, (company_name,))
        return [row[0] for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()





        


def get_liquidity_analysis():
    """
    Piyasa likiditesini analiz eder.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            WITH daily_stats AS (
                SELECT 
                    DATE_TRUNC('day', p.listing_at) AS date,
                    c.alias AS company,
                    SUM(p.share_count) AS total_volume,
                    AVG(p.unit_price) AS avg_price,
                    COUNT(DISTINCT p.id) AS transaction_count
                FROM placards p
                JOIN companies c ON p.company_id = c.id
                WHERE p.listing_at >= NOW() - INTERVAL '30 days'
                GROUP BY DATE_TRUNC('day', p.listing_at), c.alias
            )
            SELECT 
                company,
                AVG(total_volume) AS avg_daily_volume,
                AVG(avg_price) AS avg_price,
                AVG(transaction_count) AS avg_daily_transactions,
                AVG(total_volume * avg_price) AS avg_daily_turnover
            FROM daily_stats
            GROUP BY company
            ORDER BY avg_daily_turnover DESC
        """)
        
        columns = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        cur.close()
        conn.close()