#!/usr/bin/env python3
"""
SEO Opportunity Monitor - ENHANCED DEBUG VERSION
Fixed: Reddit scanning, Telegram conflicts, timeout handling
"""
import os
import sys
import re
import time
import json
import asyncio
import logging
from datetime import datetime, timedelta, UTC
from typing import List, Dict, Set, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from logging.handlers import RotatingFileHandler
import pandas as pd
import asyncpraw
import aiohttp

from dotenv import load_dotenv

# ----------------------------- Configuration -----------------------------
load_dotenv()

# Enhanced logging
log_handler = RotatingFileHandler('seo_monitor.log', maxBytes=5*1024*1024, backupCount=3)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)-8s | %(funcName)-20s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[log_handler, logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# API Keys
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT', 'giottus-seo-monitor/1.0')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Configuration
MAX_POST_AGE_HOURS = 24
KEYWORD_FILE = os.getenv('KEYWORD_CSV_PATH', 'crypto_broad-match.xlsx')
DAILY_REPORT_TIME = "09:30"
STATE_FILE = "monitor_state.json"
DAILY_STATS_FILE = "daily_stats.json"
CONTROL_FILE = "monitor_control.json"
COMMAND_POLL_INTERVAL = 10  # Increased to reduce conflicts
SCAN_INTERVAL = 300
DEBUG_LOG_FILE = "debug_events.jsonl"

# Keywords per run
PRIMARY_KEYWORDS_PER_RUN = 10
SECONDARY_KEYWORDS_PER_RUN = 20

# Competitors
COMPETITORS = {
    'international': ['Binance', 'Coinbase', 'Kraken', 'Crypto.com', 'Gemini', 'KuCoin',
                     'OKX', 'Bybit', 'MEXC', 'Uphold', 'Bitfinex', 'Bitmart', 'Bitstamp'],
    'indian': ['CoinDCX', 'Mudrex', 'CoinSwitch', 'ZebPay', 'Unocoin', 'Bitbns', 'WazirX'],
    'dex': ['Uniswap', 'PancakeSwap', 'dYdX', 'Curve Finance', 'DODO', 'KyberSwap']
}

# Spam keywords
SPAM_KEYWORDS = [
    'buy cheap', 'discount', 'promo code', 'referral link', 'sign up bonus',
    'affiliate', 'click here', 'limited offer', 'get paid', 'earn money fast',
    'make money', 'guaranteed profit', 'trading signals', 'pump and dump',
    'moonshot', 'lambo', '[STORE]', '[SELLING]', '[AD]', 'DM me',
    'telegram group', 'buy now', 'sale', 'shop', 'coupon', 'deal'
]

# ----------------------------- Debug Logger -----------------------------
class DebugLogger:
    def __init__(self, debug_file: str = DEBUG_LOG_FILE):
        self.debug_file = Path(debug_file)
        self._lock = asyncio.Lock()
    
    async def log_event(self, event_type: str, data: Dict):
        async with self._lock:
            event = {
                'timestamp': datetime.now(UTC).isoformat(),
                'event_type': event_type,
                'data': data
            }
            try:
                with open(self.debug_file, 'a') as f:
                    f.write(json.dumps(event) + '\n')
                logger.debug(f"DEBUG EVENT: {event_type}")
            except Exception as e:
                logger.error(f"Failed to log debug event: {e}")

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
    subreddit: Optional[str] = None
    keyword_priority: str = "secondary"
    india_related: bool = False
    
    def _escape_markdown_v2(self, text: str) -> str:
        if not text:
            return ""
        special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
        for char in special_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    def to_telegram_message(self) -> str:
        emoji = "⭐" if self.keyword_priority == "primary" else "📢"
        if self.india_related:
            emoji += " 🇮🇳"
        
        safe_title = self._escape_markdown_v2(self.title[:150])
        safe_author = self._escape_markdown_v2(self.author)
        safe_platform = self._escape_markdown_v2(self.platform.upper())
        
        msg = f"{emoji} *New Opportunity on {safe_platform}*\n\n"
        msg += f"📝 *Title:* {safe_title}\n\n"
        
        if self.subreddit:
            safe_subreddit = self._escape_markdown_v2(self.subreddit)
            msg += f"📍 *Subreddit:* r/{safe_subreddit}\n"
        
        msg += f"👤 *Author:* u/{safe_author}\n"
        msg += f"🔗 [View Post]({self.url})\n\n"
        
        if self.matched_keywords:
            safe_keywords = [self._escape_markdown_v2(kw) for kw in self.matched_keywords[:5]]
            keywords_str = ", ".join(safe_keywords)
            if len(self.matched_keywords) > 5:
                keywords_str += f" \\+{len(self.matched_keywords)-5} more"
            msg += f"🎯 *Keywords:* {keywords_str}\n"
        
        if self.matched_competitors:
            safe_comps = [self._escape_markdown_v2(c) for c in self.matched_competitors[:3]]
            comps_str = ", ".join(safe_comps)
            msg += f"👁️ *Competitor Mentions:* {comps_str}\n"
        
        if self.engagement:
            score = self.engagement.get('score', 0)
            comments = self.engagement.get('num_comments', 0)
            msg += f"💬 *Engagement:* ↑{score} \\| 💬{comments} comments\n"
        
        msg += f"\n⏰ *Posted:* {self._escape_markdown_v2(self._format_time())}"
        return msg
    
    def _format_time(self) -> str:
        dt = datetime.fromtimestamp(self.created_utc)
        now = datetime.now()
        delta = now - dt
        
        if delta.total_seconds() < 3600:
            mins = int(delta.total_seconds() / 60)
            return f"{mins} minutes ago"
        elif delta.total_seconds() < 86400:
            hours = int(delta.total_seconds() / 3600)
            return f"{hours} hours ago"
        else:
            return dt.strftime("%Y-%m-%d %H:%M")
    
    def to_dict(self) -> Dict:
        return asdict(self)

# ----------------------------- Keyword Manager -----------------------------
class KeywordManager:
    def __init__(self, keywords_df: pd.DataFrame):
        self.df = keywords_df
        self.primary_keywords: List[Tuple[str, int]] = []
        self.secondary_keywords: List[Tuple[str, int]] = []
        self.all_keywords: Set[str] = set()
        self.competitor_pattern = None
        self.india_pattern = None
        self.debug_logger = DebugLogger()
        self._process_keywords()
        self._build_patterns()
    
    def _process_keywords(self):
        keyword_col = None
        volume_col = None
        
        possible_kw_cols = ['keyword', 'Keyword', 'keywords', 'Keywords', 'term', 'Term']
        for col in possible_kw_cols:
            if col in self.df.columns:
                keyword_col = col
                break
        
        if not keyword_col:
            keyword_col = self.df.columns[0]
        
        possible_vol_cols = ['volume', 'Volume', 'search volume', 'Search Volume']
        for col in possible_vol_cols:
            if col in self.df.columns:
                volume_col = col
                break
        
        logger.info(f"Using keyword column: '{keyword_col}'")
        if volume_col:
            logger.info(f"Using volume column: '{volume_col}'")
        
        keyword_volume_pairs = []
        for idx, row in self.df.iterrows():
            kw = str(row[keyword_col]).strip().lower()
            if not kw or len(kw) <= 1 or kw == 'nan':
                continue
            
            if volume_col:
                try:
                    volume = int(row[volume_col]) if pd.notna(row[volume_col]) else 0
                except:
                    volume = 0
            else:
                volume = 0
            
            keyword_volume_pairs.append((kw, volume))
        
        kw_dict = {}
        for kw, vol in keyword_volume_pairs:
            if kw not in kw_dict or vol > kw_dict[kw]:
                kw_dict[kw] = vol
        
        sorted_keywords = sorted(kw_dict.items(), key=lambda x: x[1], reverse=True)
        
        self.primary_keywords = sorted_keywords[:10]
        self.secondary_keywords = sorted_keywords[10:]
        self.all_keywords = set([kw for kw, _ in sorted_keywords])
        
        logger.info(f"✓ Loaded {len(self.all_keywords)} total keywords")
        logger.info(f" - {len(self.primary_keywords)} primary (top 10 by volume)")
        logger.info(f" - {len(self.secondary_keywords)} secondary")
        logger.info(f"✓ Top 10 primary keywords: {[kw for kw, vol in self.primary_keywords]}")
    
    def _build_patterns(self):
        all_competitors = []
        for comp_list in COMPETITORS.values():
            all_competitors.extend(comp_list)
        
        competitor_terms = [re.escape(c.lower()) for c in all_competitors]
        self.competitor_pattern = re.compile(r'\b(' + '|'.join(competitor_terms) + r')\b', re.IGNORECASE)
        
        india_terms = ['india', 'indian', 'inr', 'rupee', 'delhi', 'mumbai',
                       'bangalore', 'bengaluru', 'kolkata', 'chennai', 'hyderabad']
        self.india_pattern = re.compile(r'\b(' + '|'.join(india_terms) + r')\b', re.IGNORECASE)
    
    def is_spam(self, text: str) -> bool:
        text_lower = text.lower()
        for spam_kw in SPAM_KEYWORDS:
            if spam_kw.lower() in text_lower:
                return True
        return False
    
    def is_india_related(self, text: str) -> bool:
        if self.india_pattern:
            return bool(self.india_pattern.search(text))
        return False
    
    def find_matches(self, text: str) -> Tuple[List[str], List[str], str, bool]:
        if not text:
            return [], [], "secondary", False
        
        text_lower = text.lower()
        matched_keywords = []
        keyword_priority = "secondary"
        
        # Check primary keywords
        for keyword, volume in self.primary_keywords:
            if len(keyword) <= 3:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text_lower):
                    matched_keywords.append(keyword)
                    keyword_priority = "primary"
            else:
                if keyword in text_lower:
                    matched_keywords.append(keyword)
                    keyword_priority = "primary"
        
        # Check secondary keywords
        for keyword, volume in self.secondary_keywords[:200]:
            if keyword in matched_keywords:
                continue
            if len(keyword) <= 3:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text_lower):
                    matched_keywords.append(keyword)
            else:
                if keyword in text_lower:
                    matched_keywords.append(keyword)
        
        matched_competitors = []
        if self.competitor_pattern:
            comp_matches = self.competitor_pattern.findall(text_lower)
            matched_competitors = list(set(comp_matches))
        
        india_related = self.is_india_related(text)
        
        logger.debug(f"MATCH RESULT: kw={len(matched_keywords)} comp={len(matched_competitors)} priority={keyword_priority} india={india_related}")
        
        return matched_keywords, matched_competitors, keyword_priority, india_related
    
    def get_keywords_for_search(self) -> Tuple[List[str], List[str]]:
        primary = [kw for kw, vol in self.primary_keywords[:PRIMARY_KEYWORDS_PER_RUN]]
        
        seed = int(datetime.now().timestamp() / 3600)
        import random
        random.seed(seed)
        
        if len(self.secondary_keywords) > SECONDARY_KEYWORDS_PER_RUN:
            secondary_sample = random.sample(
                [kw for kw, vol in self.secondary_keywords],
                SECONDARY_KEYWORDS_PER_RUN
            )
        else:
            secondary_sample = [kw for kw, vol in self.secondary_keywords]
        
        return primary, secondary_sample

# ----------------------------- Telegram Alerter -----------------------------
class TelegramAlerter:
    def __init__(self):
        self.bot_token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.session = None
        self.enabled = bool(self.bot_token and self.chat_id)
        self.last_send_time = 0
        self.min_interval = 2.0
        self.debug_logger = DebugLogger()
        
        if not self.enabled:
            logger.warning("⚠️ Telegram credentials not found")
    
    async def ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def send_alert(self, opportunity: SEOOpportunity):
        if not self.enabled:
            logger.info(f"🔔 {opportunity.platform.upper()}: {opportunity.title[:60]}")
            return
        
        now = time.time()
        time_since_last = now - self.last_send_time
        if time_since_last < self.min_interval:
            await asyncio.sleep(self.min_interval - time_since_last)
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            message = opportunity.to_telegram_message()
            
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'MarkdownV2',
                'disable_web_page_preview': False
            }
            
            async with self.session.post(url, json=payload, timeout=10) as resp:
                self.last_send_time = time.time()
                
                if resp.status == 200:
                    logger.info(f"✓ Alert sent: {opportunity.title[:50]}")
                else:
                    error_text = await resp.text()
                    logger.error(f"Telegram error {resp.status}: {error_text[:200]}")
        
        except Exception as e:
            logger.error(f"Failed to send alert: {e}")
    
    async def send_report(self, report_text: str):
        if not self.enabled:
            logger.info("📊 Daily report (not sent - no Telegram config)")
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': report_text,
                'parse_mode': 'MarkdownV2',
                'disable_web_page_preview': True
            }
            
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    logger.info("✓ Daily report sent successfully")
                else:
                    error_text = await resp.text()
                    logger.error(f"Failed to send report: {error_text}")
        
        except Exception as e:
            logger.error(f"Error sending report: {e}")
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

# ----------------------------- State Management -----------------------------
class StateManager:
    def __init__(self, state_file: str = STATE_FILE):
        self.state_file = Path(state_file)
        self.seen_posts: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self.load_state()
    
    def load_state(self):
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    data = json.load(f)
                    cutoff_time = time.time() - (MAX_POST_AGE_HOURS * 3600)
                    self.seen_posts = {
                        pid: timestamp for pid, timestamp in data.items()
                        if timestamp > cutoff_time
                    }
                    logger.info(f"Loaded {len(self.seen_posts)} seen posts")
            except Exception as e:
                logger.error(f"Error loading state: {e}")
                self.seen_posts = {}
        else:
            self.seen_posts = {}
    
    async def save_state(self):
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

# ----------------------------- Daily Stats Manager -----------------------------
class DailyStatsManager:
    def __init__(self, stats_file: str = DAILY_STATS_FILE):
        self.stats_file = Path(stats_file)
        self.stats = self.load_stats()
    
    def load_stats(self) -> Dict:
        today = datetime.now().strftime("%Y-%m-%d")
        default_stats = {
            'date': today,
            'reddit_count': 0,
            'primary_keyword_count': 0,
            'india_related_count': 0,
            'opportunities': [],
            'keywords': {},
            'competitors': {}
        }
        
        if self.stats_file.exists():
            try:
                with open(self.stats_file, 'r') as f:
                    all_stats = json.load(f)
                    if all_stats.get('date') == today:
                        for key in default_stats:
                            if key not in all_stats:
                                all_stats[key] = default_stats[key]
                        return all_stats
            except Exception as e:
                logger.error(f"Error loading stats: {e}")
        
        return default_stats
    
    def add_opportunity(self, opp: Dict):
        self.stats['opportunities'].append(opp)
        self.stats['reddit_count'] += 1
        
        if opp.get('keyword_priority') == 'primary':
            self.stats['primary_keyword_count'] += 1
        
        if opp.get('india_related'):
            self.stats['india_related_count'] += 1
        
        for kw in opp.get('matched_keywords', []):
            self.stats['keywords'][kw] = self.stats['keywords'].get(kw, 0) + 1
        
        for comp in opp.get('matched_competitors', []):
            self.stats['competitors'][comp] = self.stats['competitors'].get(comp, 0) + 1
    
    def save_stats(self):
        try:
            with open(self.stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")
    
    def should_send_report(self) -> bool:
        now = datetime.now()
        target_time = datetime.strptime(DAILY_REPORT_TIME, "%H:%M").time()
        current_minutes = now.hour * 60 + now.minute
        target_minutes = target_time.hour * 60 + target_time.minute
        return abs(current_minutes - target_minutes) <= 30
    
    def generate_report(self) -> str:
        total = self.stats['reddit_count']
        
        # Properly escape ALL special characters for MarkdownV2
        date_str = self.stats['date'].replace('-', '\\-')
        
        report = f"📊 *Daily SEO Report {date_str}*\n"
        report += "\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n\n"
        report += f"*Total Opportunities:* {total}\n"
        report += f"• Reddit: {self.stats['reddit_count']}\n"
        report += f"• High\\-Priority: {self.stats['primary_keyword_count']}\n"
        report += f"• India\\-Related: {self.stats['india_related_count']}\n\n"
        
        if self.stats['keywords']:
            report += "*🎯 Top Keywords:*\n"
            top_kw = sorted(self.stats['keywords'].items(), key=lambda x: x[1], reverse=True)[:5]
            for kw, count in top_kw:
                # Escape special characters in keywords
                safe_kw = kw
                for char in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
                    safe_kw = safe_kw.replace(char, f'\\{char}')
                report += f" • {safe_kw}: {count}\n"
            report += "\n"
        
        if self.stats['competitors']:
            report += "*👁️ Competitor Mentions:*\n"
            top_comp = sorted(self.stats['competitors'].items(), key=lambda x: x[1], reverse=True)[:5]
            for comp, count in top_comp:
                safe_comp = comp
                for char in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
                    safe_comp = safe_comp.replace(char, f'\\{char}')
                report += f" • {safe_comp}: {count}\n"
            report += "\n"
        
        report += "\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\\=\n"
        current_time = datetime.now().strftime('%H:%M').replace(':', '\\:')
        report += f"_Report generated at {current_time}_"
        
        return report
    
    def reset_for_new_day(self):
        today = datetime.now().strftime("%Y-%m-%d")
        if self.stats['date'] != today:
            self.stats = {
                'date': today,
                'reddit_count': 0,
                'primary_keyword_count': 0,
                'india_related_count': 0,
                'opportunities': [],
                'keywords': {},
                'competitors': {}
            }
            self.save_stats()

# ----------------------------- Control Manager -----------------------------
class ControlManager:
    def __init__(self, control_file: str = CONTROL_FILE):
        self.control_file = Path(control_file)
        self.is_running = True
        self.last_command = 'start'
        self.last_command_time = time.time()
        self.load_control()
    
    def load_control(self):
        if self.control_file.exists():
            try:
                with open(self.control_file, 'r') as f:
                    data = json.load(f)
                    self.is_running = data.get('is_running', True)
                    self.last_command = data.get('last_command', 'start')
                    self.last_command_time = data.get('last_command_time', time.time())
                    logger.info(f"Loaded control state: running={self.is_running}")
            except Exception as e:
                logger.error(f"Error loading control state: {e}")
                self.is_running = True
                self.save_control()
    
    def save_control(self):
        try:
            data = {
                'is_running': self.is_running,
                'last_command': self.last_command,
                'last_command_time': self.last_command_time
            }
            with open(self.control_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving control state: {e}")
    
    def start(self):
        self.is_running = True
        self.last_command = 'start'
        self.last_command_time = time.time()
        self.save_control()
    
    def stop(self):
        self.is_running = False
        self.last_command = 'stop'
        self.last_command_time = time.time()
        self.save_control()
    
    def should_run(self) -> bool:
        return self.is_running

# ----------------------------- Telegram Command Handler -----------------------------
class TelegramCommandHandler:
    def __init__(self, bot_token: str, chat_id: str, control_manager: ControlManager, 
                 stats_manager: DailyStatsManager, alerter: TelegramAlerter):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.control = control_manager
        self.stats = stats_manager
        self.alerter = alerter
        self.session = None
        self.last_update_id = self._load_last_update_id()
        self.last_command_time = self._load_last_command_time()
        self.processed_update_ids = set()
    
    @staticmethod
    async def clear_webhook(bot_token: str):
        """Clear any existing webhook"""
        if not bot_token:
            return
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{bot_token}/deleteWebhook"
                params = {'drop_pending_updates': True}
                async with session.post(url, params=params, timeout=10) as resp:
                    if resp.status == 200:
                        logger.info("✓ Cleared Telegram webhook and pending updates")
                    else:
                        logger.debug(f"Webhook clear status: {resp.status}")
        except Exception as e:
            logger.debug(f"Could not clear webhook: {e}")
    
    def _load_last_update_id(self) -> int:
        update_file = Path("last_update_id.json")
        if update_file.exists():
            try:
                with open(update_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_update_id', 0)
            except:
                return 0
        return 0
    
    def _save_last_update_id(self, update_id: int):
        try:
            with open("last_update_id.json", 'w') as f:
                json.dump({
                    'last_update_id': update_id,
                    'last_command_time': self.last_command_time
                }, f)
        except Exception as e:
            logger.error(f"Error saving update ID: {e}")
    
    def _load_last_command_time(self) -> float:
        update_file = Path("last_update_id.json")
        if update_file.exists():
            try:
                with open(update_file, 'r') as f:
                    data = json.load(f)
                    return data.get('last_command_time', self.control.last_command_time)
            except:
                pass
        return self.control.last_command_time
    
    async def ensure_session(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def check_commands(self):
        if not self.bot_token or not self.chat_id:
            return
        
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.bot_token}/getUpdates"
            params = {
                'offset': self.last_update_id + 1,
                'timeout': 30,
                'allowed_updates': ['message']
            }
            
            async with self.session.get(url, params=params, timeout=40) as resp:
                if resp.status == 409:
                    logger.warning("⚠️ Telegram 409 conflict - another instance may be running")
                    await asyncio.sleep(30)
                    return
                
                if resp.status != 200:
                    logger.debug(f"Telegram getUpdates failed: {resp.status}")
                    return
                
                data = await resp.json()
                if not data.get('ok'):
                    return
                
                updates = data.get('result', [])
                logger.debug(f"✓ Received {len(updates)} Telegram updates")
                
                for update in updates:
                    update_id = update['update_id']
                    
                    if update_id in self.processed_update_ids:
                        continue
                    
                    self.processed_update_ids.add(update_id)
                    self.last_update_id = update_id
                    
                    message = update.get('message', {})
                    text = message.get('text', '').strip().lower()
                    chat_id = str(message.get('chat', {}).get('id', ''))
                    message_time = message.get('date', 0)
                    
                    if chat_id != self.chat_id:
                        continue
                    
                    if message_time <= self.last_command_time:
                        logger.debug(f"Ignoring old command: {text}")
                        continue
                    
                    logger.info(f"🎯 PROCESSING COMMAND: {text} (update_id={update_id})")
                    
                    if text in ['/start', 'start']:
                        if self.control.is_running:
                            await self._send_message("⚠️ *Monitor is already running*")
                            continue
                        await self._handle_start()
                    elif text in ['/stop', 'stop']:
                        if not self.control.is_running:
                            await self._send_message("⚠️ *Monitor is already stopped*")
                            continue
                        await self._handle_stop()
                    elif text in ['/status', 'status']:
                        await self._handle_status()
                    elif text in ['/help', 'help', '/commands']:
                        await self._handle_help()
                    
                    self.last_command_time = message_time
                    self._save_last_update_id(self.last_update_id)
        
        except Exception as e:
            logger.error(f"Error checking commands: {e}")
    
    async def _handle_start(self):
        self.control.start()
        message = (
            "✅ *Monitoring Started*\n\n"
            "The SEO monitor is now active\\. "
            "You'll receive real\\-time alerts for all opportunities\\.\n\n"
            f"Started at: {datetime.now().strftime('%Y\\-%m\\-%d %H\\:%M')}"
        )
        await self._send_message(message)
        
        report = self.stats.generate_report()
        await self.alerter.send_report(report)
        
        logger.info("✅ Monitoring started via Telegram command")
    
    async def _handle_stop(self):
        self.control.stop()
        message = (
            "⏸️ *Monitoring Stopped*\n\n"
            "The SEO monitor is now stopped\\. "
            "No new alerts will be sent\\.\n\n"
            f"Stopped at: {datetime.now().strftime('%Y\\-%m\\-%d %H\\:%M')}\n\n"
            "Send /start to resume monitoring\\."
        )
        await self._send_message(message)
        logger.info("⏸️ Monitoring stopped via Telegram command")
    
    async def _handle_status(self):
        status = "🟢 Running" if self.control.is_running else "🔴 Stopped"
        last_cmd_time = datetime.fromtimestamp(self.control.last_command_time).strftime('%Y-%m-%d %H:%M')
        
        # Escape special characters
        last_cmd_time = last_cmd_time.replace('-', '\\-').replace(':', '\\:')
        
        message = (
            f"📊 *Monitor Status*\n\n"
            f"*Status:* {status}\n"
            f"*Last Command:* /{self.control.last_command}\n"
            f"*Command Time:* {last_cmd_time}\n\n"
        )
        
        if self.control.is_running:
            message += "✅ Monitoring is active\n"
            message += "Send /stop to pause"
        else:
            message += "⏸️ Monitoring is stopped\n"
            message += "Send /start to resume"
        
        await self._send_message(message)
    
    async def _handle_help(self):
        message = (
            "🤖 *SEO Monitor Bot Commands*\n\n"
            "*Available Commands:*\n"
            "/start \\- Start monitoring and receive daily report\n"
            "/stop \\- Stop monitoring\n"
            "/status \\- Check current status\n"
            "/help \\- Show this help message\n\n"
            "*About:*\n"
            "This bot monitors Reddit for crypto discussions "
            "and sends real\\-time alerts for every opportunity found\\.\n\n"
            "Focus: Brand visibility and awareness opportunities\\.\n"
            "Daily reports sent at 09:30 and on /start\\."
        )
        await self._send_message(message)
    
    async def _send_message(self, text: str):
        try:
            await self.ensure_session()
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            
            payload = {
                'chat_id': self.chat_id,
                'text': text,
                'parse_mode': 'MarkdownV2'
            }
            
            async with self.session.post(url, json=payload, timeout=10) as resp:
                if resp.status != 200:
                    error = await resp.text()
                    logger.error(f"Failed to send message: {error}")
        
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def run_command_loop(self):
        """Run continuous command checking loop"""
        logger.info("🎮 Starting Telegram command loop")
        while True:
            try:
                await self.check_commands()
                await asyncio.sleep(COMMAND_POLL_INTERVAL)
            except asyncio.CancelledError:
                logger.info("Command loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in command loop: {e}")
                await asyncio.sleep(10)

# ----------------------------- Reddit Monitor -----------------------------
class RedditMonitor:
    def __init__(self, keyword_manager, alerter, state, stats, control_manager):
        self.km = keyword_manager
        self.alerter = alerter
        self.state = state
        self.stats = stats
        self.control = control_manager
        self.reddit = None
    
    async def _init_reddit(self):
        self.reddit = asyncpraw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        logger.info("✓ Reddit client initialized")
    
    def _is_fresh(self, created_utc: float) -> bool:
        age_seconds = time.time() - created_utc
        return age_seconds <= (MAX_POST_AGE_HOURS * 3600)
    
    async def scan(self, primary_keywords, secondary_keywords):
        await self._init_reddit()
        found_count = 0
        all_keywords = primary_keywords + secondary_keywords
        
        logger.info(f"🔍 REDDIT SCAN START: {len(primary_keywords)} primary + {len(secondary_keywords)} secondary keywords")
        
        for idx, kw in enumerate(all_keywords, 1):
            if not self.control.should_run():
                logger.warning("⚠️ Reddit scan interrupted - monitor stopped")
                break
            
            is_primary = kw in primary_keywords
            kw_type = "PRIMARY" if is_primary else "secondary"
            
            logger.info(f"📝 [{idx}/{len(all_keywords)}] Searching: '{kw}' ({kw_type})")
            
            try:
                subreddit = await self.reddit.subreddit('all')
                
                submission_count = 0
                search_start_time = time.time()
                
                async for submission in subreddit.search(
                    kw,
                    sort='new',
                    time_filter='day',
                    limit=15
                ):
                    # Timeout check
                    if time.time() - search_start_time > 45:
                        logger.warning(f"⏱️ Search timeout for '{kw}' after 45 seconds")
                        break
                    
                    if not self.control.should_run():
                        logger.warning("⚠️ Submission processing interrupted")
                        break
                    
                    submission_count += 1
                    unique_post_id = f"reddit_{submission.id}"
                    
                    logger.debug(f"  [{submission_count}] Post: {submission.title[:60]}")
                    
                    # Quick seen check
                    if await self.state.is_seen(unique_post_id):
                        logger.debug(f"    ↳ SKIPPED: Already seen")
                        continue
                    
                    # Check freshness
                    if not self._is_fresh(submission.created_utc):
                        logger.debug(f"    ↳ SKIPPED: Too old")
                        await self.state.mark_seen(unique_post_id)
                        continue
                    
                    # Process with timeout
                    try:
                        processed = await asyncio.wait_for(
                            self._process_submission(submission, unique_post_id, is_primary),
                            timeout=10.0
                        )
                        
                        if processed:
                            found_count += 1
                            logger.info(f"    ✅ OPPORTUNITY FOUND! Total today: {found_count}")
                    
                    except asyncio.TimeoutError:
                        logger.warning(f"    ⏱️ TIMEOUT processing: {submission.title[:60]}")
                        await self.state.mark_seen(unique_post_id)
                    
                    except Exception as e:
                        logger.error(f"    ❌ ERROR processing: {e}")
                        await self.state.mark_seen(unique_post_id)
                    
                    # Small delay between posts
                    await asyncio.sleep(0.2)
                
                logger.info(f"  ✓ Processed {submission_count} submissions for '{kw}'")
            
            except Exception as e:
                logger.error(f"❌ Reddit search error for '{kw}': {e}")
                await asyncio.sleep(2.0)
            
            # Delay between keywords
            await asyncio.sleep(1.0)
        
        logger.info(f"🎯 REDDIT SCAN COMPLETE - Found {found_count} opportunities")
        
        if self.reddit:
            await self.reddit.close()
        
        return found_count
    
    async def _process_submission(self, submission, unique_post_id, is_primary):
        """Process a single submission"""
        try:
            # Get text content
            title = str(submission.title) if submission.title else ""
            selftext = str(submission.selftext) if hasattr(submission, 'selftext') and submission.selftext else ""
            text = f"{title} {selftext}".strip()
            
            if not text:
                return False
            
            # Spam check
            if self.km.is_spam(text):
                logger.debug(f"    ↳ FILTERED: Spam detected")
                await self.state.mark_seen(unique_post_id)
                return False
            
            # Find matches
            keywords, competitors, priority, india_related = self.km.find_matches(text)
            
            # Only process if we have matches
            if not (keywords or competitors):
                logger.debug(f"    ↳ NO MATCH: No keywords/competitors found")
                return False
            
            logger.info(f"    🎯 MATCH: kw={len(keywords)} comp={len(competitors)} priority={priority} india={india_related}")
            
            # Mark as seen immediately
            await self.state.mark_seen(unique_post_id)
            
            # Get engagement
            try:
                engagement = {
                    'score': getattr(submission, 'score', 0),
                    'num_comments': getattr(submission, 'num_comments', 0),
                    'upvote_ratio': getattr(submission, 'upvote_ratio', 0.0)
                }
            except:
                engagement = {'score': 0, 'num_comments': 0, 'upvote_ratio': 0.0}
            
            # Get author
            try:
                author = str(submission.author) if submission.author else 'deleted'
            except:
                author = 'unknown'
            
            # Get subreddit
            try:
                subreddit_name = str(submission.subreddit)
            except:
                subreddit_name = 'unknown'
            
            # Create opportunity
            opportunity = SEOOpportunity(
                platform='reddit',
                title=title[:200],
                url=f"https://reddit.com{submission.permalink}",
                content=text[:500],
                matched_keywords=keywords[:10],
                matched_competitors=competitors[:5],
                timestamp=datetime.now(UTC).isoformat(),
                post_id=unique_post_id,
                author=author,
                created_utc=submission.created_utc,
                engagement=engagement,
                subreddit=subreddit_name,
                keyword_priority=priority,
                india_related=india_related
            )
            
            # Send alert
            try:
                await asyncio.wait_for(
                    self.alerter.send_alert(opportunity),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning("    ⏱️ Timeout sending alert")
            except Exception as e:
                logger.error(f"    ❌ Error sending alert: {e}")
            
            # Update stats
            self.stats.add_opportunity(opportunity.to_dict())
            
            return True
        
        except Exception as e:
            logger.error(f"    ❌ Error in _process_submission: {e}")
            await self.state.mark_seen(unique_post_id)
            return False
    
    async def close(self):
        if self.reddit:
            try:
                await self.reddit.close()
                logger.info("✓ Reddit client closed")
            except Exception as e:
                logger.debug(f"Error closing Reddit: {e}")

# ----------------------------- Main Monitor Loop -----------------------------
async def monitor_loop():
    logger.info("=" * 80)
    logger.info("🚀 SEO MONITOR - ENHANCED DEBUG VERSION")
    logger.info("=" * 80)
    
    if not os.path.exists(KEYWORD_FILE):
        logger.error(f"❌ Keyword file not found: {KEYWORD_FILE}")
        sys.exit(1)
    
    command_task = None
    state_manager = None
    stats_manager = None
    control_manager = None
    alerter = None
    command_handler = None
    reddit_monitor = None
    
    try:
        # Load keywords
        logger.info("📂 Loading keyword file...")
        if KEYWORD_FILE.endswith('.xlsx') or KEYWORD_FILE.endswith('.xls'):
            df = pd.read_excel(KEYWORD_FILE)
        else:
            df = pd.read_csv(KEYWORD_FILE)
        logger.info(f"✓ Loaded {len(df)} keywords from file")
        
        # Initialize components
        logger.info("⚙️ Initializing components...")
        keyword_manager = KeywordManager(df)
        state_manager = StateManager()
        stats_manager = DailyStatsManager()
        control_manager = ControlManager()
        alerter = TelegramAlerter()
        
        # Clear webhook
        logger.info("🧹 Clearing Telegram webhook...")
        await TelegramCommandHandler.clear_webhook(TELEGRAM_BOT_TOKEN)
        
        # Initialize command handler
        logger.info("🎮 Initializing Telegram command handler...")
        command_handler = TelegramCommandHandler(
            TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, control_manager, stats_manager, alerter
        )
        command_task = asyncio.create_task(command_handler.run_command_loop())
        
        # Initialize Reddit monitor
        logger.info("🔧 Initializing Reddit monitor...")
        reddit_monitor = RedditMonitor(keyword_manager, alerter, state_manager, stats_manager, control_manager)
        
        logger.info("=" * 80)
        logger.info("✅ ALL SYSTEMS READY - Starting monitoring loop")
        logger.info("=" * 80)
        
        last_report_check = datetime.now()
        cycle_number = 0
        
        while True:
            try:
                # Check if monitoring is paused
                if not control_manager.should_run():
                    logger.info("⏸️ Monitoring PAUSED - Waiting for /start command...")
                    await asyncio.sleep(COMMAND_POLL_INTERVAL)
                    continue
                
                cycle_number += 1
                logger.info(f"\n{'='*80}")
                logger.info(f"🔄 SCAN CYCLE #{cycle_number} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*80}")
                
                # Reset stats for new day
                stats_manager.reset_for_new_day()
                
                # Get keywords for this cycle
                primary_kw, secondary_kw = keyword_manager.get_keywords_for_search()
                
                logger.info(f"📋 Keywords for this cycle:")
                logger.info(f"  • Primary ({len(primary_kw)}): {primary_kw[:3]}...")
                logger.info(f"  • Secondary ({len(secondary_kw)}): {secondary_kw[:3]}...")
                
                # Run Reddit scan
                reddit_count = await reddit_monitor.scan(primary_kw, secondary_kw)
                
                # Save state and stats
                await state_manager.save_state()
                stats_manager.save_stats()
                control_manager.save_control()
                
                # Log summary
                logger.info(f"\n{'='*80}")
                logger.info(f"📊 CYCLE #{cycle_number} SUMMARY:")
                logger.info(f"  • Opportunities found this cycle: {reddit_count}")
                logger.info(f"  • Total opportunities today: {stats_manager.stats['reddit_count']}")
                logger.info(f"  • High-priority: {stats_manager.stats.get('primary_keyword_count', 0)}")
                logger.info(f"  • India-related: {stats_manager.stats.get('india_related_count', 0)}")
                logger.info(f"{'='*80}\n")
                
                # Check for daily report
                now = datetime.now()
                if (now - last_report_check).total_seconds() >= 60:
                    if stats_manager.should_send_report():
                        logger.info("📊 Sending daily report...")
                        report = stats_manager.generate_report()
                        await alerter.send_report(report)
                        
                        archive_file = f"daily_stats_{stats_manager.stats['date']}.json"
                        stats_manager.stats_file.rename(archive_file)
                        logger.info(f"✓ Archived stats to {archive_file}")
                    
                    last_report_check = now
                
                # Wait for next cycle
                logger.info(f"⏳ Waiting {SCAN_INTERVAL} seconds before next cycle...")
                await asyncio.sleep(SCAN_INTERVAL)
            
            except asyncio.CancelledError:
                logger.info("Main loop cancelled")
                break
            except Exception as e:
                logger.exception(f"❌ ERROR in scan cycle: {e}")
                logger.info("⏳ Waiting 60 seconds before retry...")
                await asyncio.sleep(60)
    
    except Exception as e:
        logger.exception(f"❌ FATAL ERROR: {e}")
    
    finally:
        logger.info("\n🛑 Initiating shutdown sequence...")
        
        # Cancel command task
        if command_task:
            command_task.cancel()
            try:
                await command_task
            except asyncio.CancelledError:
                pass
        
        # Close all connections
        cleanup_tasks = []
        if command_handler:
            cleanup_tasks.append(command_handler.close())
        if alerter:
            cleanup_tasks.append(alerter.close())
        if reddit_monitor:
            cleanup_tasks.append(reddit_monitor.close())
        if state_manager:
            cleanup_tasks.append(state_manager.save_state())
        
        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        
        # Save final state
        if stats_manager:
            stats_manager.save_stats()
        if control_manager:
            control_manager.save_control()
        
        logger.info("✓ Monitor shut down cleanly")


if __name__ == '__main__':
    try:
        asyncio.run(monitor_loop())
    except KeyboardInterrupt:
        logger.info("\n⚠️ Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"❌ Application error: {e}")
        sys.exit(1)