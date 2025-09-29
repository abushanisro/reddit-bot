# 📊 SEO Opportunity Monitor

A production-ready **real-time SEO opportunity tracker** that monitors Reddit for crypto-related discussions.
It automatically identifies keyword matches, competitor mentions, and India-specific opportunities, and sends alerts to Telegram.

---

## 🚀 Features

* 🔍 **Keyword-based search** from a CSV/Excel list
* ⭐ **Primary vs Secondary keyword prioritization**
* 👁️ **Competitor monitoring** across major global crypto exchanges
* 🇮🇳 **India-specific filtering** with toggleable modes
* 📢 **Real-time Telegram alerts** with Markdown formatting
* ⏸️ **Start/Stop control via Telegram commands**
* 📊 **Daily stats tracking** (keywords, competitors, geo-distribution)
* 🛡️ **Spam/ad filtering** using regex patterns

---

## 📂 Project Structure

```
.
├── main.py                 # Main script (SEO monitor)
├── requirements.txt        # Dependencies
├── crypto_broad-match.xlsx # Keywords file (default)
├── seo_monitor.log         # Rotating log file
├── monitor_state.json      # Stores seen posts
├── monitor_control.json    # Stores bot control state
├── daily_stats.json        # Tracks daily opportunities
└── README.md               # Documentation
```

---

## ⚙️ Setup

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/seo-opportunity-monitor.git
cd seo-opportunity-monitor
```

### 2. Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file in the project root:

```env
# Reddit API
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret
REDDIT_USER_AGENT=seo-monitor/2.0

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=-100xxxxxxxxxx   # Your group/channel ID

# Settings
KEYWORD_CSV_PATH=crypto_broad-match.xlsx
```

---

## ▶️ Usage

Run the monitor:

```bash
python main.py
```

The bot will:

* Load keywords from the Excel file
* Scan Reddit for new posts (primary + rotating secondary keywords)
* Send alerts for relevant matches to your Telegram chat
* Track stats and control state in JSON files

---

## 🤖 Telegram Commands

* `/start` → Resume monitoring
* `/stop` → Pause monitoring
* `/status` → Show current status
* `/india` → Switch to India-only mode + show India report
* `/global` → Switch to global mode + show global report
* `/help` → Show help menu

---

## 📊 Reports

* **India Report** → Top India-specific opportunities, keywords, and competitors
* **Global Report** → Keyword trends, competitor mentions, and geo split
* Both reports are generated on-demand via `/india` or `/global`

---

## 🛡️ Filters

* **Competitor subreddits** (Binance, Coinbase, WazirX, etc.) are excluded automatically
* **Spam/ad content** filtered using regex (referrals, promotions, “moonshot” posts, etc.)

---

## 📝 Logs

* Logs are stored in `seo_monitor.log` (rotating, 10MB × 5 backups)
* Console output mirrors log events

---

## 📌 Requirements

* Python 3.9+
* Reddit API credentials → [Get from Reddit Apps](https://www.reddit.com/prefs/apps)
* Telegram Bot + Chat ID → Create with [BotFather](https://t.me/BotFather)

---

## 📈 Example Alert

```
⭐ New Opportunity on REDDIT

📝 Title: Best crypto exchanges in India 2025?
📍 Subreddit: r/CryptoIndia
👤 Author: u/exampleuser
🔗 View Post: https://reddit.com/xyz

🎯 Keywords: crypto exchange, bitcoin, trading
👁️ Competitors: Binance, WazirX
💬 Engagement: ↑42 | 💬15 comments
```
