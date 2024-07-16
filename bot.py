import logging
import asyncio
from uuid import uuid4
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InlineQueryResultArticle, InputTextMessageContent
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, InlineQueryHandler
from config import TOKEN, NEW_DATA_CHECK_INTERVAL, ADMIN_USER_IDS
import queries

# Enable logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
last_checked_id = queries.get_latest_transaction_id()

def split_message(message, max_length=4000):
    return [message[i:i+max_length] for i in range(0, len(message), max_length)]

# Command handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Merhaba! Ben gelişmiş finansal veri monitörü botuyum.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    help_text = """
    Mevcut komutlar:
    /start - Botu başlat
    /help - Yardım mesajını göster
    /about - Bot hakkında bilgi
    /query <şirket adı> - Belirli bir şirket için son 5 işlemi sorgula
    /companies - Tüm şirketleri listele
    /stats <şirket adı> - Belirli bir şirket için istatistikler
    /price_history <şirket adı> [gün sayısı] - Belirli bir şirket için fiyat geçmişi
    /top_companies [limit] - İşlem hacmine göre en yüksek şirketleri listele
    /price_trend <şirket adı> <daily/weekly/monthly> - Belirli bir şirket için fiyat trendi
    /share_distribution <şirket adı> - Bir şirketin pay grubu dağılımını göster
    /compare_companies <şirket1> <şirket2> ... - Birden fazla şirketi karşılaştır
    /market_sentiment [gün sayısı] - Piyasa duyarlılığını analiz et
    /market_anomalies [eşik değeri] - Piyasadaki anormal hareketleri tespit et
    /price_predictions [gün sayısı] - Gelecek fiyat trendlerini tahmin et
    /correlated_companies [eşik değeri] - Yüksek korelasyonlu şirketleri bul
    /market_efficiency - Piyasa etkinliğini ölç
    /liquidity_analysis - Piyasa likiditesini analiz et
    /subscribe <şirket adı> - Belirli bir şirket için bildirim almaya başla
    /subscribe_all - Tüm şirketler için bildirim almaya başla
    /unsubscribe_all - Tüm bildirimleri devre dışı bırak
    /admin - Yönetici paneli (sadece yöneticiler için)
    """
    for part in split_message(help_text):
        await update.message.reply_text(part)


async def subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    company_name = " ".join(context.args) if context.args else None
    
    if not company_name:
        await update.message.reply_text("Lütfen abone olmak istediğiniz şirket adını girin. Örnek: /subscribe NOOK")
        return

    result = queries.add_subscription(user_id, company_name)
    if result:
        await update.message.reply_text(f"{company_name} için aboneliğiniz başarıyla eklendi.")
    else:
        await update.message.reply_text("Abonelik eklenirken bir hata oluştu. Lütfen tekrar deneyin.")

async def subscribe_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    result = queries.add_subscription(user_id, 'ALL')
    if result:
        await update.message.reply_text("Tüm şirketler için bildirimler başarıyla etkinleştirildi.")
    else:
        await update.message.reply_text("Abonelik eklenirken bir hata oluştu. Lütfen tekrar deneyin.")

async def unsubscribe_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    result = queries.remove_subscription(user_id, 'ALL')
    if result:
        await update.message.reply_text("Tüm şirketler için bildirimler devre dışı bırakıldı.")
    else:
        await update.message.reply_text("Abonelik kaldırılırken bir hata oluştu. Lütfen tekrar deneyin.")




async def about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    about_text = "Bu bot, finansal verileri takip etmek, analiz etmek ve otomatik hesaplamalar yapmak için tasarlanmıştır."
    for part in split_message(about_text):
        await update.message.reply_text(part)

async def query_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Lütfen bir şirket adı girin. Örnek: /query Company A")
        return

    company_name = " ".join(context.args)
    results = queries.get_latest_transactions(company_name)
    
    if not results.empty:
        response = f"{company_name} için son 5 işlem:\n\n"
        for _, row in results.iterrows():
            response += f"ID: {row['transaction_id']}, Şirket: {row['Şirket']}, Fiyat: {row['unitPrice']}, Pay Adedi: {row['shareCount']}, Tarih: {row['listingAt']}\n"
    else:
        response = f"{company_name} için sonuç bulunamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def list_companies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    companies = queries.get_all_companies()
    if companies:
        response = "Mevcut şirketler:\n\n" + "\n".join(companies)
    else:
        response = "Henüz hiç şirket kaydedilmemiş."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def company_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Lütfen bir şirket adı girin. Örnek: /stats Company A")
        return

    company_name = " ".join(context.args)
    stats = queries.get_company_stats(company_name)
    
    if not stats.empty:
        stats = stats.iloc[0]
        response = f"{company_name} istatistikleri:\n\n"
        response += f"İşlem Sayısı: {stats['İşlem Sayısı']}\n"
        response += f"Ortalama Fiyat: {stats['Ortalama Fiyat']:.2f}\n"
        response += f"Toplam Pay Adedi: {stats['Toplam Pay Adedi']}\n"
        response += f"İlk İşlem Tarihi: {stats['İlk İşlem Tarihi']}\n"
        response += f"Son İşlem Tarihi: {stats['Son İşlem Tarihi']}\n"
    else:
        response = f"{company_name} için istatistik bulunamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def price_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Lütfen bir şirket adı girin. Örnek: /price_history Company A [gün sayısı]")
        return

    company_name = " ".join(context.args[:-1]) if len(context.args) > 1 else context.args[0]
    days = int(context.args[-1]) if len(context.args) > 1 and context.args[-1].isdigit() else 30

    history = queries.get_price_history(company_name, days)
    
    if not history.empty:
        response = f"{company_name} için son {days} günlük fiyat geçmişi:\n\n"
        for _, row in history.iterrows():
            response += f"Tarih: {row['Tarih']}, Fiyat: {row['Fiyat']}\n"
    else:
        response = f"{company_name} için fiyat geçmişi bulunamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if user_id in ADMIN_USER_IDS:
        keyboard = [
            [InlineKeyboardButton("Özet Rapor Oluştur", callback_data='generate_report')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text('Yönetici Paneli:', reply_markup=reply_markup)
    else:
        await update.message.reply_text("Bu komutu kullanma yetkiniz yok.")

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == 'generate_report':
        await query.message.reply_text("Özet rapor oluşturuluyor...")
        # Burada rapor oluşturma mantığını ekleyebilirsiniz

async def inline_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.inline_query.query
    if not query:
        return
    results = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title="Şirket Sorgula",
            input_message_content=InputTextMessageContent(f"Şirket sorgusu: {query}")
        )
    ]
    await update.inline_query.answer(results)




async def check_new_data_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    global last_checked_id
    last_checked_id = int(last_checked_id)
    new_data = queries.get_new_transactions(last_checked_id)
    
    if not new_data.empty:
        logger.info(f"{len(new_data)} yeni kayıt bulundu.")
        
        for _, row in new_data.iterrows():
            process_type = "Alış" if row['processType'] == 'buy' else "Satış"
            total_amount = row['shareCount'] * row['unitPrice']
            
            message = f"{process_type} - {row['Şirket']} - {row['share_group_letter']}\n\n"
            message += f"İşlem Adedi: {row['shareCount']:,.0f}\n"
            message += f"Fiyat: {row['unitPrice']:,.2f}\n"
            message += f"Toplam Tutar: {total_amount:,.2f}"
            
            subscribed_users = queries.get_subscribed_users(row['Şirket'])
            
            for user_id in subscribed_users:
                try:
                    await context.bot.send_message(chat_id=user_id, text=message)
                except Exception as e:
                    logger.error(f"Kullanıcı {user_id}'ye mesaj gönderirken hata oluştu: {e}")
        
        last_checked_id = new_data['transaction_id'].max()
    else:
        logger.info("Yeni işlem bulunamadı.")



# Yeni komut işleyicileri
async def market_anomalies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    threshold = 2  # Varsayılan eşik değeri
    if context.args and context.args[0].isdigit():
        threshold = float(context.args[0])
    
    results = queries.get_market_anomalies(threshold)
    
    if not results.empty:
        response = f"Piyasa Anomalileri (Eşik: {threshold}):\n\n"
        for _, row in results.iterrows():
            response += f"Şirket: {row['company']}, Durum: {row['status']}\n"
            response += f"Ort. Fiyat: {row['avg_price']:.2f}, Std. Sapma: {row['stddev_price']:.2f}\n"
            response += f"Max: {row['max_price']:.2f}, Min: {row['min_price']:.2f}\n\n"
    else:
        response = "Hiçbir anormal hareket tespit edilmedi."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def price_predictions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = 30  # Varsayılan tahmin süresi
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    
    results = queries.predict_price_trends(days)
    
    if not results.empty:
        response = f"Gelecek {days} gün için fiyat tahminleri:\n\n"
        for company in results['company'].unique():
            company_data = results[results['company'] == company]
            last_price = company_data['predicted_price'].iloc[-1]
            first_price = company_data['predicted_price'].iloc[0]
            change_percent = ((last_price - first_price) / first_price) * 100
            
            response += f"{company}:\n"
            response += f"Başlangıç: {first_price:.2f}, Son: {last_price:.2f}\n"
            response += f"Değişim: {change_percent:.2f}%\n\n"
    else:
        response = "Tahmin yapılamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def correlated_companies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    threshold = 0.7  # Varsayılan korelasyon eşiği
    if context.args and context.args[0].replace('.', '').isdigit():
        threshold = float(context.args[0])
    
    results = queries.get_correlated_companies(threshold)
    
    if not results.empty:
        response = f"Yüksek Korelasyonlu Şirketler (Eşik: {threshold}):\n\n"
        for _, row in results.iterrows():
            response += f"{row['company1']} - {row['company2']}: {row['correlation']:.2f}\n"
    else:
        response = f"{threshold} eşiğinin üzerinde korelasyona sahip şirket çifti bulunamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def market_sentiment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    results = queries.get_market_sentiment(days)
    
    if not results.empty:
        response = f"Son {days} günün piyasa duyarlılığı:\n\n"
        for _, row in results.iterrows():
            response += f"Tarih: {row['Tarih']}, Alım: {row['Alım Sayısı']}, Satım: {row['Satım Sayısı']}, Duyarlılık: {row['Duyarlılık']}, Ort. Fiyat: {row['Ortalama Fiyat']:.2f}\n"
    else:
        response = f"Son {days} günün piyasa duyarlılığı bulunamadı. Lütfen daha sonra tekrar deneyin."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def liquidity_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    results = queries.get_liquidity_analysis()
    
    if not results.empty:
        response = "Piyasa Likidite Analizi:\n\n"
        for _, row in results.iterrows():
            response += f"Şirket: {row['company']}\n"
            response += f"Ort. Günlük Hacim: {row['avg_daily_volume']:.2f}\n"
            response += f"Ort. Fiyat: {row['avg_price']:.2f}\n"
            response += f"Ort. Günlük İşlem Sayısı: {row['avg_daily_transactions']:.2f}\n"
            response += f"Ort. Günlük Ciro: {row['avg_daily_turnover']:.2f}\n\n"
    else:
        response = "Likidite analizi yapılamadı."
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def top_companies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    limit = 10
    if context.args and context.args[0].isdigit():
        limit = int(context.args[0])
    results = queries.get_top_companies_by_transaction_volume(limit)
    response = "İşlem hacmine göre en yüksek şirketler:\n\n"
    for _, row in results.iterrows():
        response += f"{row['Şirket']}: İşlem Sayısı: {row['İşlem Sayısı']}, Toplam Hacim: {row['Toplam Hacim']:.2f}\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def price_trend(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Kullanım: /price_trend <şirket adı> <daily/weekly/monthly>")
        return
    company_name = " ".join(context.args[:-1])
    interval = context.args[-1]
    results = queries.get_price_trend(company_name, interval)
    response = f"{company_name} için {interval} fiyat trendi:\n\n"
    for _, row in results.iterrows():
        response += f"Tarih: {row['Tarih']}, Ort: {row['Ortalama Fiyat']:.2f}, Min: {row['En Düşük Fiyat']:.2f}, Max: {row['En Yüksek Fiyat']:.2f}\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def share_distribution(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Lütfen bir şirket adı girin.")
        return
    company_name = " ".join(context.args)
    results = queries.get_share_group_distribution(company_name)
    response = f"{company_name} için pay grubu dağılımı:\n\n"
    for _, row in results.iterrows():
        response += f"Grup: {row['Pay Grubu']}, İşlem: {row['İşlem Sayısı']}, Toplam Pay: {row['Toplam Pay Adedi']}, Ort. Fiyat: {row['Ortalama Fiyat']:.2f}\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def compare_companies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Lütfen en az iki şirket adı girin.")
        return
    results = queries.get_company_comparison(context.args)
    response = "Şirket Karşılaştırması:\n\n"
    for _, row in results.iterrows():
        response += f"Şirket: {row['Şirket']}\n"
        response += f"Toplam İşlem: {row['Toplam İşlem Sayısı']}\n"
        response += f"Ortalama Fiyat: {row['Ortalama Fiyat']:.2f}\n"
        response += f"Toplam Pay: {row['Toplam Pay Adedi']}\n"
        response += f"Toplam Hacim: {row['Toplam İşlem Hacmi']:.2f}\n\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def market_sentiment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    results = queries.get_market_sentiment(days)
    response = f"Son {days} günün piyasa duyarlılığı:\n\n"
    for _, row in results.iterrows():
        response += f"Tarih: {row['Tarih']}, Alım: {row['Alım Sayısı']}, Satım: {row['Satım Sayısı']}, Duyarlılık: {row['Duyarlılık']}, Ort. Fiyat: {row['Ortalama Fiyat']:.2f}\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def market_anomalies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    threshold = 2
    if context.args and context.args[0].replace('.', '').isdigit():
        threshold = float(context.args[0])
    results = queries.get_market_anomalies(threshold)
    response = f"Piyasa Anomalileri (Eşik: {threshold}):\n\n"
    for _, row in results.iterrows():
        response += f"Şirket: {row['company']}, Durum: {row['status']}\n"
        response += f"Ort. Fiyat: {row['avg_price']:.2f}, Std. Sapma: {row['stddev_price']:.2f}\n"
        response += f"Max: {row['max_price']:.2f}, Min: {row['min_price']:.2f}\n\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def price_predictions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    days = 30
    if context.args and context.args[0].isdigit():
        days = int(context.args[0])
    results = queries.predict_price_trends(days)
    response = f"Gelecek {days} gün için fiyat tahminleri:\n\n"
    for company in results['company'].unique():
        company_data = results[results['company'] == company]
        last_price = company_data['predicted_price'].iloc[-1]
        first_price = company_data['predicted_price'].iloc[0]
        change_percent = ((last_price - first_price) / first_price) * 100
        response += f"{company}: Başlangıç: {first_price:.2f}, Son: {last_price:.2f}, Değişim: {change_percent:.2f}%\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def correlated_companies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    threshold = 0.7
    if context.args and context.args[0].replace('.', '').isdigit():
        threshold = float(context.args[0])
    results = queries.get_correlated_companies(threshold)
    response = f"Yüksek Korelasyonlu Şirketler (Eşik: {threshold}):\n\n"
    for _, row in results.iterrows():
        response += f"{row['company1']} - {row['company2']}: {row['correlation']:.2f}\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def market_efficiency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    results = queries.get_market_efficiency()
    response = "Piyasa Etkinliği Analizi:\n\n"
    response += f"Hurst Exponent: {results['Hurst Exponent'].iloc[0]:.2f}\n"
    response += f"Yorum: {results['Yorum'].iloc[0]}"
    for part in split_message(response):
        await update.message.reply_text(part)

async def liquidity_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    results = queries.get_liquidity_analysis()
    response = "Piyasa Likidite Analizi:\n\n"
    for _, row in results.iterrows():
        response += f"Şirket: {row['company']}\n"
        response += f"Ort. Günlük Hacim: {row['avg_daily_volume']:.2f}\n"
        response += f"Ort. Fiyat: {row['avg_price']:.2f}\n"
        response += f"Ort. Günlük İşlem: {row['avg_daily_transactions']:.2f}\n"
        response += f"Ort. Günlük Ciro: {row['avg_daily_turnover']:.2f}\n\n"
    
    for part in split_message(response):
        await update.message.reply_text(part)

async def main() -> None:
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("about", about))
    application.add_handler(CommandHandler("query", query_command))
    application.add_handler(CommandHandler("companies", list_companies))
    application.add_handler(CommandHandler("stats", company_stats))
    application.add_handler(CommandHandler("price_history", price_history))
    application.add_handler(CommandHandler("admin", admin_panel))
    application.add_handler(CommandHandler("subscribe", subscribe))
    application.add_handler(CommandHandler("subscribe_all", subscribe_all))
    application.add_handler(CommandHandler("unsubscribe_all", unsubscribe_all))
    # Yeni otomatik analiz komutları
    application.add_handler(CommandHandler("market_anomalies", market_anomalies))
    application.add_handler(CommandHandler("price_predictions", price_predictions))
    application.add_handler(CommandHandler("correlated_companies", correlated_companies))
    application.add_handler(CommandHandler("market_efficiency", market_efficiency))
    application.add_handler(CommandHandler("liquidity_analysis", liquidity_analysis))

    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(InlineQueryHandler(inline_query))

    # Yeni komutlar
    application.add_handler(CommandHandler("top_companies", top_companies))
    application.add_handler(CommandHandler("price_trend", price_trend))
    application.add_handler(CommandHandler("share_distribution", share_distribution))
    application.add_handler(CommandHandler("compare_companies", compare_companies))
    application.add_handler(CommandHandler("market_sentiment", market_sentiment))

    job_queue = application.job_queue
    job_queue.run_repeating(check_new_data_job, interval=NEW_DATA_CHECK_INTERVAL, first=10)

    await application.initialize()
    await application.start()
    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    logger.info("Bot başlatıldı. Durdurmak için Ctrl+C tuşlarına basın.")
    
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Bot durdurulma isteği alındı...")
    finally:
        await application.stop()
        await application.shutdown()
        logger.info("Bot durduruldu.")

if __name__ == "__main__":
    asyncio.run(main())
