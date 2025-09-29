#!/usr/bin/env python3
"""
SEO Opportunity Monitor - Production Version
Real-time Telegram control, no duplicates, keyword-based search
"""
import os
import sys
import re
import time
import json
import asyncio
import logging
from datetime import datetime, UTC
from typing import List, Dict, Set, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from logging.handlers import RotatingFileHandler
import pandas as pd
import asyncpraw
import aiohttp
from dotenv import load_dotenv

# ----------------------------- Configuration -----------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        RotatingFileHandler('seo_monitor.log', maxBytes=10*1024*1024, backupCount=5),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# API Keys
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'seo-monitor/2.0')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Settings
MAX_POST_AGE_HOURS = 24
KEYWORD_FILE = os.getenv('KEYWORD_CSV_PATH', 'crypto_broad-match.xlsx')
STATE_FILE = "monitor_state.json"
CONTROL_FILE = "monitor_control.json"
STATS_FILE = "daily_stats.json"
COMMAND_POLL_INTERVAL = 5
SCAN_INTERVAL = 300
PRIMARY_KW_COUNT = 10
SECONDARY_KW_PER_CYCLE = 20

# Competitors - BLOCKED SUBREDDITS
COMPETITOR_SUBREDDITS = {
    'binance', 'coinbase', 'kraken', 'cryptocom', 'gemini', 'kucoin',
    'okx', 'bybit', 'mexc', 'uphold', 'bitfinex', 'bitmart', 'bitstamp',
    'etoro', 'robinhood', 'bitflyer', 'gateio', 'cexio', 'htx',
    'coindcx', 'mudrex', 'coinswitch', 'zebpay', 'unocoin', 'bitbns',
    'wazirx', 'paxful', 'uniswap', 'pancakeswap', 'dydx', 'curvefi',
    'dodoex', 'kyberswap'
}

# Competitor names for awareness tracking
COMPETITORS = {
    'Binance', 'Coinbase', 'Kraken', 'Crypto.com', 'Gemini', 'KuCoin',
    'OKX', 'Bybit', 'MEXC', 'Uphold', 'Bitfinex', 'Bitmart', 'Bitstamp',
    'eToro', 'Robinhood', 'BitFlyer', 'Gate.io', 'CEX.io', 'HTX',
    'CoinDCX', 'Mudrex', 'CoinSwitch', 'ZebPay', 'Unocoin', 'Bitbns',
    'WazirX', 'Paxful', 'Uniswap', 'PancakeSwap', 'dYdX', 'Curve Finance',
    'DODO', 'KyberSwap'
}

# Spam patterns for ad filtering
SPAM_PATTERNS = [
    r'\b(buy now|click here|limited offer|promo code|referral|affiliate)\b',
    r'\b(discount|sale|shop|coupon|deal|earn money|get paid)\b',
    r'\[(store|selling|ad|buy)\]',
    r'\b(dm me|telegram group|whatsapp)\b',
    r'\b(guaranteed profit|moonshot|lambo|pump)\b'
]

# ----------------------------- Data Models -----------------------------
@dataclass
class SEOOpportunity:
    platform: str
    title: str
    url: str
    content: str
    matched_keywords: List[str]
    matched_competitors: List[str]
    timestamp: str
    post_id: str
    author: str
    created_utc: float
    engagement: Dict
    subreddit: str
    keyword_priority: str
    india_related: bool
    
    def _escape_md(self, text: str) -> str:
        if not text:
            return ""
        chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for c in chars:
            text = text.replace(c, f'\\{c}')
        return text
    
    def to_telegram_message(self) -> str:
        emoji = "⭐" if self.keyword_priority == "primary" else "📢"
        if self.india_related:
            emoji += " 🇮🇳"
        
        title = self._escape_md(self.title[:150])
        author = self._escape_md(self.author)
        subreddit = self._escape_md(self.subreddit)
        
        msg = f"{emoji} *New Opportunity on REDDIT*\n\n"
        msg += f"📝 *Title:* {title}\n\n"
        msg += f"📍 *Subreddit:* r/{subreddit}\n"
        msg += f"👤 *Author:* u/{author}\n"
        msg += f"🔗 [View Post]({self.url})\n\n"
        
        if self.matched_keywords:
            kws = [self._escape_md(k) for k in self.matched_keywords[:5]]
            kw_str = ", ".join(kws)
            if len(self.matched_keywords) > 5:
                kw_str += f" \\+{len(self.matched_keywords)-5} more"
            msg += f"🎯 *Keywords:* {kw_str}\n"
        
        if self.matched_competitors:
            comps = [self._escape_md(c) for c in self.matched_competitors[:3]]
            comp_str = ", ".join(comps)
            if len(self.matched_competitors) > 3:
                comp_str += f" \\+{len(self.matched_competitors)-3} more"
            msg += f"👁️ *Competitors:* {comp_str}\n"
        
        score = self.engagement.get('score', 0)
        comments = self.engagement.get('num_comments', 0)
        msg += f"💬 *Engagement:* ↑{score} \\| 💬{comments} comments\n"
        
        # Show snippet of content if competitors mentioned
        if self.matched_competitors and self.content:
            snippet = self._escape_md(self.content[:200])
            msg += f"\n📄 *Snippet:* {snippet}\\.\\.\\.\n"
        
        return msg

# ----------------------------- Keyword Manager -----------------------------
class KeywordManager:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.primary_keywords = []
        self.secondary_keywords = []
        self.competitor_pattern = None
        self.india_pattern = None
        self._process_keywords()
        self._build_patterns()
    
    def _process_keywords(self):
        kw_col = next((c for c in ['Keyword', 'keyword', 'Keywords'] if c in self.df.columns), self.df.columns[0])
        vol_col = next((c for c in ['Volume', 'volume', 'Search Volume'] if c in self.df.columns), None)
        
        logger.info(f"Using keyword column: '{kw_col}'")
        if vol_col:
            logger.info(f"Using volume column: '{vol_col}'")
        
        kw_dict = {}
        for _, row in self.df.iterrows():
            kw = str(row[kw_col]).strip().lower()
            if not kw or len(kw) <= 1 or kw == 'nan':
                continue
            
            vol = 0
            if vol_col:
                try:
                    vol = int(row[vol_col]) if pd.notna(row[vol_col]) else 0
                except:
                    vol = 0
            
            if kw not in kw_dict or vol > kw_dict[kw]:
                kw_dict[kw] = vol
        
        sorted_kws = sorted(kw_dict.items(), key=lambda x: x[1], reverse=True)
        self.primary_keywords = sorted_kws[:PRIMARY_KW_COUNT]
        self.secondary_keywords = sorted_kws[PRIMARY_KW_COUNT:]
        
        logger.info(f"✓ Loaded {len(sorted_kws)} keywords")
        logger.info(f"  - {len(self.primary_keywords)} primary")
        logger.info(f"  - {len(self.secondary_keywords)} secondary")
        logger.info(f"✓ Top primary: {[k for k, _ in self.primary_keywords[:5]]}")
    
    def _build_patterns(self):
        comp_terms = [re.escape(c.lower()) for c in COMPETITORS]
        self.competitor_pattern = re.compile(r'\b(' + '|'.join(comp_terms) + r')\b', re.I)
        
        india_terms = ['india', 'indian', 'inr', 'rupee', 'delhi', 'mumbai', 
                       'bangalore', 'bengaluru', 'kolkata', 'chennai', 'hyderabad']
        self.india_pattern = re.compile(r'\b(' + '|'.join(india_terms) + r')\b', re.I)
    
    def is_spam(self, text: str) -> bool:
        text_lower = text.lower()
        return any(re.search(pattern, text_lower) for pattern in SPAM_PATTERNS)
    
    def is_india_related(self, text: str) -> bool:
        return bool(self.india_pattern.search(text))
    
    def find_matches(self, text: str) -> Tuple[List[str], List[str], str, bool]:
        if not text:
            return [], [], "secondary", False
        
        text_lower = text.lower()
        matched_kws = []
        priority = "secondary"
        
        # Check primary keywords first
        for kw, _ in self.primary_keywords:
            if len(kw) <= 3:
                if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
                    matched_kws.append(kw)
                    priority = "primary"
            else:
                if kw in text_lower:
                    matched_kws.append(kw)
                    priority = "primary"
        
        # Check secondary keywords
        for kw, _ in self.secondary_keywords[:200]:
            if kw in matched_kws:
                continue
            if len(kw) <= 3:
                if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
                    matched_kws.append(kw)
            else:
                if kw in text_lower:
                    matched_kws.append(kw)
        
        matched_comps = []
        if self.competitor_pattern:
            comps = self.competitor_pattern.findall(text_lower)
            matched_comps = list(set(comps))
        
        india = self.is_india_related(text)
        
        return matched_kws, matched_comps, priority, india
    
    def get_search_keywords(self) -> Tuple[List[str], List[str]]:
        primary = [k for k, _ in self.primary_keywords]
        
        import random
        random.seed(int(time.time() / 3600))
        
        if len(self.secondary_keywords) > SECONDARY_KW_PER_CYCLE:
            secondary = random.sample([k for k, _ in self.secondary_keywords], SECONDARY_KW_PER_CYCLE)
        else:
            secondary = [k for k, _ in self.secondary_keywords]
        
        return primary, secondary

# ----------------------------- Stats Manager -----------------------------
class StatsManager:
    def __init__(self):
        self.stats_file = Path(STATS_FILE)
        self.opportunities = []
        self.load()
    
    def load(self):
        if self.stats_file.exists():
            try:
                with open(self.stats_file) as f:
                    data = json.load(f)
                    today = datetime.now().strftime('%Y-%m-%d')
                    if data.get('date') == today:
                        self.opportunities = data.get('opportunities', [])
                logger.info(f"Loaded {len(self.opportunities)} opportunities from today")
            except Exception as e:
                logger.error(f"Error loading stats: {e}")
    
    def add_opportunity(self, opp: SEOOpportunity):
        self.opportunities.append(asdict(opp))
        self.save()
    
    def save(self):
        try:
            with open(self.stats_file, 'w') as f:
                json.dump({
                    'date': datetime.now().strftime('%Y-%m-%d'),
                    'opportunities': self.opportunities
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")
    
    def get_india_opportunities(self) -> List[Dict]:
        return [opp for opp in self.opportunities if opp.get('india_related', False)]
    
    def reset_if_new_day(self):
        today = datetime.now().strftime('%Y-%m-%d')
        if self.stats_file.exists():
            try:
                with open(self.stats_file) as f:
                    data = json.load(f)
                    if data.get('date') != today:
                        self.opportunities = []
                        self.save()
            except:
                pass

# ----------------------------- State Manager -----------------------------
class StateManager:
    def __init__(self):
        self.state_file = Path(STATE_FILE)
        self.seen_posts = {}
        self._lock = asyncio.Lock()
        self.load()
    
    def load(self):
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                    cutoff = time.time() - (MAX_POST_AGE_HOURS * 3600)
                    self.seen_posts = {k: v for k, v in data.items() if v > cutoff}
                logger.info(f"Loaded {len(self.seen_posts)} seen posts")
            except Exception as e:
                logger.error(f"Error loading state: {e}")
    
    async def save(self):
        async with self._lock:
            try:
                with open(self.state_file, 'w') as f:
                    json.dump(self.seen_posts, f)
            except Exception as e:
                logger.error(f"Error saving state: {e}")
    
    async def is_seen(self, post_id: str) -> bool:
        async with self._lock:
            return post_id in self.seen_posts
    
    async def mark_seen(self, post_id: str):
        async with self._lock:
            self.seen_posts[post_id] = time.time()

# ----------------------------- Control Manager -----------------------------
class ControlManager:
    def __init__(self):
        self.file = Path(CONTROL_FILE)
        self.running = True
        self.india_only = False
        self.load()
    
    def load(self):
        if self.file.exists():
            try:
                with open(self.file) as f:
                    data = json.load(f)
                    self.running = data.get('running', True)
                    self.india_only = data.get('india_only', False)
                logger.info(f"Control state: running={self.running}, india_only={self.india_only}")
            except Exception as e:
                logger.error(f"Error loading control: {e}")
    
    def save(self):
        try:
            with open(self.file, 'w') as f:
                json.dump({
                    'running': self.running,
                    'india_only': self.india_only,
                    'updated': time.time()
                }, f)
        except Exception as e:
            logger.error(f"Error saving control: {e}")
    
    def start(self):
        self.running = True
        self.save()
    
    def stop(self):
        self.running = False
        self.save()
    
    def set_india_only(self):
        self.india_only = True
        self.save()
    
    def set_global(self):
        self.india_only = False
        self.save()
    
    def should_run(self) -> bool:
        return self.running

# ----------------------------- Telegram Handler -----------------------------
class TelegramHandler:
    def __init__(self, control: ControlManager, stats: 'StatsManager'):
        self.token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.control = control
        self.stats = stats
        self.session = None
        self.last_update_id = 0
        self.enabled = bool(self.token and self.chat_id)
        
        if not self.enabled:
            logger.warning("⚠️ Telegram not configured")
    
    async def ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def send_alert(self, opp: SEOOpportunity):
        if not self.enabled:
            logger.info(f"🔔 {opp.title[:60]}")
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': opp.to_telegram_message(),
                'parse_mode': 'MarkdownV2',
                'disable_web_page_preview': False
            }
            
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    logger.info(f"✓ Alert sent: {opp.title[:50]}")
                else:
                    error = await resp.text()
                    logger.error(f"Telegram error: {error[:200]}")
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def check_commands(self):
        if not self.enabled:
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            params = {'offset': self.last_update_id + 1, 'timeout': 20}
            
            async with self.session.get(url, params=params, timeout=30) as resp:
                if resp.status != 200:
                    return
                
                data = await resp.json()
                if not data.get('ok'):
                    return
                
                for update in data.get('result', []):
                    update_id = update['update_id']
                    self.last_update_id = update_id
                    
                    msg = update.get('message', {})
                    text = msg.get('text', '').strip().lower()
                    chat = str(msg.get('chat', {}).get('id', ''))
                    
                    if chat != self.chat_id:
                        continue
                    
                    logger.info(f"🎯 Command received: {text}")
                    
                    if text in ['/start', 'start']:
                        await self._handle_start()
                    elif text in ['/stop', 'stop']:
                        await self._handle_stop()
                    elif text in ['/status', 'status']:
                        await self._handle_status()
                    elif text in ['/india', 'india']:
                        await self._handle_india_report()
                    elif text in ['/global', 'global']:
                        await self._handle_global()
                    elif text in ['/help', 'help', '/commands']:
                        await self._handle_help()
        
        except Exception as e:
            logger.debug(f"Command check error: {e}")
    
    async def _handle_start(self):
        was_stopped = not self.control.running
        self.control.start()
        
        mode = "India\\-only" if self.control.india_only else "Global"
        
        msg = "✅ *Monitoring STARTED*\n\n"
        if was_stopped:
            msg += "Resuming real\\-time monitoring\\.\n"
        else:
            msg += "Already running\\.\n"
        msg += f"*Mode:* {mode}\n"
        msg += f"Status updated: {datetime.now().strftime('%H\\:%M')}"
        
        await self._send_message(msg)
        logger.info(f"✅ Monitoring started (mode: {mode})")
    
    async def _handle_stop(self):
        was_running = self.control.running
        self.control.stop()
        
        msg = "⏸️ *Monitoring STOPPED*\n\n"
        if was_running:
            msg += "Monitoring paused\\. No new alerts will be sent\\.\n"
        else:
            msg += "Already stopped\\.\n"
        msg += f"Status updated: {datetime.now().strftime('%H\\:%M')}\n\n"
        msg += "Send /start to resume\\."
        
        await self._send_message(msg)
        logger.info("⏸️ Monitoring stopped via /stop")
    
    async def _handle_status(self):
        status = "🟢 Running" if self.control.running else "🔴 Stopped"
        mode = "🇮🇳 India\\-only" if self.control.india_only else "🌍 Global"
        
        msg = f"📊 *Monitor Status*\n\n"
        msg += f"*Status:* {status}\n"
        msg += f"*Mode:* {mode}\n\n"
        
        if self.control.running:
            msg += "Monitoring is active\\.\n"
            msg += "Send /stop to pause\\.\n\n"
        else:
            msg += "Monitoring is paused\\.\n"
            msg += "Send /start to resume\\.\n\n"
        
        msg += "*Available Commands:*\n"
        msg += "/start \\- Resume monitoring\n"
        msg += "/stop \\- Pause monitoring\n"
        msg += "/india \\- India report \\& switch to India\\-only\n"
        msg += "/global \\- Global report \\& switch to global\n"
        msg += "/status \\- Check status"
        
        await self._send_message(msg)
    
    async def _handle_global(self):
        """Switch to global mode and show global report"""
        self.control.set_global()
        
        all_opps = self.stats.opportunities
        india_opps = self.stats.get_india_opportunities()
        global_opps = [o for o in all_opps if not o.get('india_related', False)]
        
        # Group by priority
        primary = [o for o in all_opps if o.get('keyword_priority') == 'primary']
        
        # Extract top keywords and competitors
        all_keywords = {}
        all_competitors = {}
        
        for opp in all_opps:
            for kw in opp.get('matched_keywords', []):
                all_keywords[kw] = all_keywords.get(kw, 0) + 1
            for comp in opp.get('matched_competitors', []):
                all_competitors[comp] = all_competitors.get(comp, 0) + 1
        
        top_kw = sorted(all_keywords.items(), key=lambda x: x[1], reverse=True)[:5]
        top_comp = sorted(all_competitors.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Build report
        msg = f"🌍 *Global Coverage Report*\n"
        msg += f"_{datetime.now().strftime('%Y\\-%m\\-%d')}_\n"
        msg += "\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n\n"
        
        msg += f"*Total Opportunities:* {len(all_opps)}\n"
        msg += f"• High\\-Priority: {len(primary)}\n"
        msg += f"• India\\-Specific: {len(india_opps)}\n"
        msg += f"• Global: {len(global_opps)}\n\n"
        
        if top_kw:
            msg += "*🎯 Top Keywords:*\n"
            for kw, count in top_kw:
                safe_kw = self._escape_md(kw)
                msg += f"  • {safe_kw}: {count}\n"
            msg += "\n"
        
        if top_comp:
            msg += "*👁️ Competitor Mentions:*\n"
            for comp, count in top_comp:
                safe_comp = self._escape_md(comp)
                msg += f"  • {safe_comp}: {count}\n"
            msg += "\n"
        
        msg += "*📊 Geographic Distribution:*\n"
        msg += f"  • India: {len(india_opps)} \\({int(len(india_opps)/len(all_opps)*100) if all_opps else 0}\\%\\)\n"
        msg += f"  • Global: {len(global_opps)} \\({int(len(global_opps)/len(all_opps)*100) if all_opps else 0}\\%\\)\n\n"
        
        msg += "\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n"
        msg += f"_Switched to GLOBAL mode_\n"
        msg += f"_Report generated at {datetime.now().strftime('%H\\:%M')}_"
        
        await self._send_message(msg)
        logger.info(f"✓ Switched to GLOBAL mode | Report sent: {len(all_opps)} opportunities")
    
    async def _handle_help(self):
        """Show help message with available commands"""
        msg = (
            "🤖 *SEO Monitor Bot*\n\n"
            "*Available Commands:*\n"
            "/start \\- Resume monitoring\n"
            "/stop \\- Pause monitoring\n"
            "/status \\- Check current status\n"
            "/india \\- India report \\& switch to India\\-only mode\n"
            "/global \\- Global report \\& switch to global mode\n"
            "/help \\- Show this help message\n\n"
            "*About:*\n"
            "This bot monitors Reddit for crypto opportunities\\.\n"
            "You'll receive real\\-time alerts for relevant discussions\\.\n\n"
            "*Features:*\n"
            "• Keyword\\-based monitoring\n"
            "• Competitor tracking\n"
            "• India\\-specific filtering\n"
            "• Real\\-time start/stop control"
        )
        await self._send_message(msg)
    
    async def _handle_india_report(self):
        """Generate India-specific opportunities report and switch to India-only mode"""
        self.control.set_india_only()
        
        india_opps = self.stats.get_india_opportunities()
        
        if not india_opps:
            msg = (
                "🇮🇳 *India Report*\n\n"
                "No India\\-specific opportunities found today\\.\n\n"
                f"_Switched to INDIA\\-ONLY mode_\n"
                f"_Report generated at {datetime.now().strftime('%H\\:%M')}_"
            )
            await self._send_message(msg)
            logger.info("✓ Switched to INDIA-ONLY mode (no opportunities yet)")
            return
        
        # Group by keyword priority
        primary = [o for o in india_opps if o.get('keyword_priority') == 'primary']
        secondary = [o for o in india_opps if o.get('keyword_priority') == 'secondary']
        
        # Extract top keywords and competitors
        all_keywords = {}
        all_competitors = {}
        
        for opp in india_opps:
            for kw in opp.get('matched_keywords', []):
                all_keywords[kw] = all_keywords.get(kw, 0) + 1
            for comp in opp.get('matched_competitors', []):
                all_competitors[comp] = all_competitors.get(comp, 0) + 1
        
        top_kw = sorted(all_keywords.items(), key=lambda x: x[1], reverse=True)[:5]
        top_comp = sorted(all_competitors.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Build report
        msg = f"🇮🇳 *India\\-Specific Report*\n"
        msg += f"_{datetime.now().strftime('%Y\\-%m\\-%d')}_\n"
        msg += "\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n\n"
        
        msg += f"*Total India Opportunities:* {len(india_opps)}\n"
        msg += f"• High\\-Priority: {len(primary)}\n"
        msg += f"• Secondary: {len(secondary)}\n\n"
        
        if top_kw:
            msg += "*🎯 Top Keywords:*\n"
            for kw, count in top_kw:
                safe_kw = self._escape_md(kw)
                msg += f"  • {safe_kw}: {count}\n"
            msg += "\n"
        
        if top_comp:
            msg += "*👁️ Competitor Mentions:*\n"
            for comp, count in top_comp:
                safe_comp = self._escape_md(comp)
                msg += f"  • {safe_comp}: {count}\n"
            msg += "\n"
        
        # Recent opportunities
        msg += "*📌 Recent Opportunities:*\n"
        for opp in india_opps[-5:]:  # Last 5
            title = self._escape_md(opp['title'][:60])
            subreddit = self._escape_md(opp['subreddit'])
            msg += f"• r/{subreddit}: {title}\\.\\.\\.\n"
        
        msg += "\n\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n"
        msg += f"_Switched to INDIA\\-ONLY mode_\n"
        msg += f"_Report generated at {datetime.now().strftime('%H\\:%M')}_"
        
        await self._send_message(msg)
        logger.info(f"✓ Switched to INDIA-ONLY mode | Report sent: {len(india_opps)} opportunities")
    
    def _escape_md(self, text: str) -> str:
        """Escape markdown special characters"""
        if not text:
            return ""
        chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for c in chars:
            text = text.replace(c, f'\\{c}')
        return text
    
    async def _send_message(self, text: str):
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': 'MarkdownV2'
            }
            
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Message send error: {error}")
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# ----------------------------- Reddit Monitor -----------------------------
class RedditMonitor:
    def __init__(self, km, tg, state, control, stats):
        self.km = km
        self.tg = tg
        self.state = state
        self.control = control
        self.stats = stats
        self.reddit = None
        self.found_today = 0
    
    async def init_reddit(self):
        self.reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
    
    async def scan(self, primary_kws, secondary_kws):
        await self.init_reddit()
        all_kws = primary_kws + secondary_kws
        
        logger.info(f"🔍 Starting scan: {len(primary_kws)} primary + {len(secondary_kws)} secondary")
        
        for idx, kw in enumerate(all_kws, 1):
            if not self.control.should_run():
                logger.warning("⚠️ Scan stopped by user command")
                break
            
            is_primary = kw in primary_kws
            logger.info(f"[{idx}/{len(all_kws)}] Searching: '{kw}' ({'PRIMARY' if is_primary else 'secondary'})")
            
            try:
                subreddit = await self.reddit.subreddit('all')
                count = 0
                
                async for sub in subreddit.search(kw, sort='new', time_filter='day', limit=15):
                    if not self.control.should_run():
                        break
                    
                    count += 1
                    post_id = f"reddit_{sub.id}"
                    
                    # Skip if seen
                    if await self.state.is_seen(post_id):
                        continue
                    
                    # Skip if too old
                    if time.time() - sub.created_utc > MAX_POST_AGE_HOURS * 3600:
                        await self.state.mark_seen(post_id)
                        continue
                    
                    # Skip competitor subreddits
                    try:
                        sub_name = str(sub.subreddit).lower()
                        if sub_name in COMPETITOR_SUBREDDITS:
                            logger.debug(f"  ↳ SKIPPED: In competitor subreddit r/{sub_name}")
                            await self.state.mark_seen(post_id)
                            continue
                    except:
                        pass
                    
                    # Process
                    try:
                        processed = await asyncio.wait_for(
                            self._process_post(sub, post_id, is_primary),
                            timeout=10.0
                        )
                        
                        if processed:
                            self.found_today += 1
                            logger.info(f"  ✅ OPPORTUNITY #{self.found_today}")
                    
                    except asyncio.TimeoutError:
                        logger.warning(f"  ⏱️ Timeout processing post")
                        await self.state.mark_seen(post_id)
                    except Exception as e:
                        logger.error(f"  ❌ Error: {e}")
                        await self.state.mark_seen(post_id)
                    
                    await asyncio.sleep(0.3)
                
                logger.info(f"  ✓ Processed {count} posts for '{kw}'")
            
            except Exception as e:
                logger.error(f"❌ Search error for '{kw}': {e}")
            
            await asyncio.sleep(1.0)
        
        if self.reddit:
            await self.reddit.close()
        
        return self.found_today
    
    async def _process_post(self, sub, post_id, is_primary):
        try:
            title = str(sub.title) if sub.title else ""
            selftext = str(sub.selftext) if hasattr(sub, 'selftext') and sub.selftext else ""
            text = f"{title} {selftext}".strip()
            
            if not text:
                return False
            
            # Spam check
            if self.km.is_spam(text):
                logger.debug(f"  ↳ FILTERED: Spam/advertising")
                await self.state.mark_seen(post_id)
                return False
            
            # Find matches
            kws, comps, priority, india = self.km.find_matches(text)
            
            # Only process if we have matches
            if not (kws or comps):
                return False
            
            # Filter by mode: India-only or Global
            if self.control.india_only and not india:
                logger.debug(f"  ↳ SKIPPED: Not India-related (India-only mode)")
                return False
            
            logger.info(f"  🎯 MATCH: {len(kws)} kw, {len(comps)} comp, {priority}, india={india}")
            
            # Mark seen immediately
            await self.state.mark_seen(post_id)
            
            # Build opportunity
            try:
                engagement = {
                    'score': getattr(sub, 'score', 0),
                    'num_comments': getattr(sub, 'num_comments', 0),
                    'upvote_ratio': getattr(sub, 'upvote_ratio', 0.0)
                }
            except:
                engagement = {'score': 0, 'num_comments': 0, 'upvote_ratio': 0.0}
            
            try:
                author = str(sub.author) if sub.author else 'deleted'
            except:
                author = 'unknown'
            
            try:
                subreddit_name = str(sub.subreddit)
            except:
                subreddit_name = 'unknown'
            
            opp = SEOOpportunity(
                platform='reddit',
                title=title[:200],
                url=f"https://reddit.com{sub.permalink}",
                content=text[:500],
                matched_keywords=kws[:10],
                matched_competitors=comps[:5],
                timestamp=datetime.now(UTC).isoformat(),
                post_id=post_id,
                author=author,
                created_utc=sub.created_utc,
                engagement=engagement,
                subreddit=subreddit_name,
                keyword_priority=priority,
                india_related=india
            )
            
            # Send alert
            try:
                await asyncio.wait_for(self.tg.send_alert(opp), timeout=10.0)
            except:
                pass
            
            # Save to stats
            self.stats.add_opportunity(opp)
            
            return True
        
        except Exception as e:
            logger.error(f"  ❌ Process error: {e}")
            await self.state.mark_seen(post_id)
            return False

# ----------------------------- Main Loop -----------------------------
async def main():
    logger.info("=" * 80)
    logger.info("🚀 SEO MONITOR - PRODUCTION VERSION")
    logger.info("=" * 80)
    
    if not os.path.exists(KEYWORD_FILE):
        logger.error(f"❌ Keyword file not found: {KEYWORD_FILE}")
        sys.exit(1)
    
    # Load keywords
    logger.info("📂 Loading keywords...")
    if KEYWORD_FILE.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(KEYWORD_FILE)
    else:
        df = pd.read_csv(KEYWORD_FILE)
    logger.info(f"✓ Loaded {len(df)} keywords")
    
    # Initialize
    km = KeywordManager(df)
    state = StateManager()
    stats = StatsManager()
    control = ControlManager()
    tg = TelegramHandler(control, stats)
    monitor = RedditMonitor(km, tg, state, control, stats)
    
    logger.info("✅ ALL SYSTEMS READY")
    logger.info("=" * 80)
    
    command_task = asyncio.create_task(command_loop(tg))
    cycle = 0
    
    try:
        while True:
            if not control.should_run():
                logger.info("⏸️ Monitoring PAUSED - waiting for /start command...")
                await asyncio.sleep(COMMAND_POLL_INTERVAL)
                continue
            
            cycle += 1
            logger.info(f"\n{'='*80}")
            logger.info(f"🔄 CYCLE #{cycle} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            mode = "🇮🇳 INDIA-ONLY" if control.india_only else "🌍 GLOBAL"
            logger.info(f"📍 Mode: {mode}")
            logger.info(f"{'='*80}")
            
            # Reset stats for new day
            stats.reset_if_new_day()
            
            primary, secondary = km.get_search_keywords()
            logger.info(f"📋 Searching {len(primary)} primary + {len(secondary)} secondary keywords")
            
            found = await monitor.scan(primary, secondary)
            
            await state.save()
            control.save()
            
            logger.info(f"📊 Cycle complete: {found} opportunities found")
            logger.info(f"⏳ Waiting {SCAN_INTERVAL}s before next cycle...\n")
            
            await asyncio.sleep(SCAN_INTERVAL)
    
    except asyncio.CancelledError:
        logger.info("Main loop cancelled")
    except Exception as e:
        logger.exception(f"❌ Fatal error: {e}")
    finally:
        logger.info("🛑 Shutting down...")
        command_task.cancel()
        try:
            await command_task
        except:
            pass
        await tg.close()
        await state.save()
        logger.info("✓ Shutdown complete")

async def command_loop(tg):
    """Separate task for checking Telegram commands"""
    while True:
        try:
            await tg.check_commands()
            await asyncio.sleep(COMMAND_POLL_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Command loop error: {e}")
            await asyncio.sleep(10)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n⚠️ Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"❌ Application error: {e}")
        sys.exit(1)
