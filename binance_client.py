"""
Binance Price Feed Client
"""

import requests
import logging
from typing import Optional
from statistics import mean
from collections import deque
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BinanceClient:
    def __init__(self):
        self.base_url = "https://api.binance.com"
        self.session = requests.Session()
        self.price_history = {}  # 存储价格历史用于计算波动率

    def get_spot_price(self, symbol: str) -> Optional[float]:
        """获取实时现货价格"""
        try:
            url = f"{self.base_url}/api/v3/ticker/price"
            response = self.session.get(url, params={"symbol": symbol}, timeout=3)
            response.raise_for_status()
            data = response.json()
            price = float(data.get("price", 0))

            # 记录历史
            if symbol not in self.price_history:
                self.price_history[symbol] = deque(maxlen=100)
            self.price_history[symbol].append({"price": price, "time": time.time()})

            return price

        except Exception as e:
            logger.error(f"Error fetching {symbol} price: {e}")
            return None

    def get_recent_trades(self, symbol: str, limit: int = 100) -> list:
        """获取最近成交记录"""
        try:
            url = f"{self.base_url}/api/v3/trades"
            response = self.session.get(
                url, params={"symbol": symbol, "limit": limit}, timeout=3
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
            return []

    def calculate_volatility(self, symbol: str, window_minutes: int = 15) -> float:
        """
        计算历史波动率（log-return 标准差）
        返回: 指定窗口内的对数收益率标准差
        """
        try:
            import math

            history = self.price_history.get(symbol, [])
            if len(history) < 10:
                # 历史数据不足，使用默认波动率
                return 0.002  # 0.2% 默认短窗波动率

            # 只取最近 window_minutes 的数据
            cutoff_time = time.time() - (window_minutes * 60)
            recent_prices = [h["price"] for h in history if h["time"] > cutoff_time]

            if len(recent_prices) < 5:
                return 0.002

            # 计算 log-returns
            log_returns = []
            for i in range(1, len(recent_prices)):
                if recent_prices[i - 1] > 0 and recent_prices[i] > 0:
                    log_returns.append(
                        math.log(recent_prices[i] / recent_prices[i - 1])
                    )

            if len(log_returns) < 3:
                return 0.002

            avg_return = mean(log_returns)
            volatility = (
                sum((r - avg_return) ** 2 for r in log_returns) / (len(log_returns) - 1)
            ) ** 0.5

            return max(volatility, 0.0001)  # 最小 0.01%

        except Exception as e:
            logger.error(f"Error calculating volatility: {e}")
            return 0.002

    def get_price_change_stats(self, symbol: str, period: str = "15m") -> dict:
        """获取价格变化统计"""
        try:
            url = f"{self.base_url}/api/v3/ticker/24hr"
            response = self.session.get(url, params={"symbol": symbol}, timeout=3)
            response.raise_for_status()
            data = response.json()

            return {
                "price_change_percent": float(data.get("priceChangePercent", 0)),
                "high_price": float(data.get("highPrice", 0)),
                "low_price": float(data.get("lowPrice", 0)),
                "volume": float(data.get("volume", 0)),
                "weighted_avg_price": float(data.get("weightedAvgPrice", 0)),
            }
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            return {}
