"""
Ranger Finance - Стратегия Усреднения/Пирамидинга для Spot торговли
=====================================================================

Этот модуль реализует стратегию с усреднением и пирамидингом для spot торговли.

Основные принципы:
- Маркет ордера на покупку с последующим отслеживанием цены для TP
- Усреднение при падении цены
- Пирамидинг при росте цены
- Локальное отслеживание позиций и TP целей

Логика работы:
1. Если нет открытых позиций → маркет покупка + запись TP цели (entry_price + STEP)
2. Усреднение: если current_price < min_TP_price - STEP * 2
3. Пирамидинг: если current_price > max_TP_price - PWR (PWR = STEP * AGGR)
4. Take Profit: продажа по маркету при достижении цены TP

Использование:
    from modules.averaging_strategy import trade_averaging_strategy
    
    await trade_averaging_strategy(
        client=your_spot_client_instance,
        token_name="WBTC"
    )
"""

from decimal import Decimal
from loguru import logger
from datetime import datetime
import asyncio
import os
import time

from .utils import round_cut, async_sleep, send_warning_notification, send_profit_notification
from .utils.tg_report import TgReport
from .spot_client import SpotClient
import settings

# Глобальные переменные для сбора стартовых балансов всех аккаунтов
_startup_balances = {}
_startup_lock = asyncio.Lock()
_startup_message_sent = False


async def send_combined_startup_message():
    """
    Отправляет объединенное стартовое сообщение со всеми аккаунтами
    """
    global _startup_message_sent
    
    if _startup_message_sent:
        return
    
    # Ждем 3 секунды, чтобы все аккаунты успели добавить свою информацию
    await async_sleep(3)
    
    async with _startup_lock:
        if _startup_message_sent:
            return
        
        if not _startup_balances:
            return
        
        # Формируем общее сообщение
        start_msg = "🚀 <b>Bot Started</b>\n\n"
        
        for label, balance_info in _startup_balances.items():
            start_msg += f"<b>{label}:</b>\n"
            limit_orders = balance_info.get('limit_orders', 0)
            if limit_orders > 0:
                start_msg += f"💰 {balance_info['usdc']:.0f} USDC + {balance_info['token']:.6f} {balance_info['token_name']} + ${limit_orders:.0f} Limit Orders = ${balance_info['total']:.0f}\n\n"
            else:
                start_msg += f"💰 {balance_info['usdc']:.0f} USDC + {balance_info['token']:.6f} {balance_info['token_name']} = ${balance_info['total']:.0f}\n\n"
        
        # Отправляем через TgReport
        try:
            await TgReport().send_log(logs=start_msg)
        except Exception as e:
            logger.debug(f"Failed to send combined startup message: {e}")
        
        _startup_message_sent = True


async def get_average_buy_price_for_amount(client: 'SpotClient', token_name: str, target_amount: Decimal) -> tuple:
    """
    Получает среднюю цену покупки для заданного объема токенов.
    Идет по истории от последней покупки к более старым, суммируя объемы
    пока не наберется target_amount.
    
    Args:
        client: SpotClient instance
        token_name: Название токена (например "WBTC")
        target_amount: Целевой объем токенов для анализа (orphaned amount)
        
    Returns:
        tuple: (average_price: float, trades_count: int) или (None, 0) если история недоступна
    """
    try:
        # Получаем историю торговли
        trades = await client.browser.get_trade_history(token_pair=f"{token_name}-USDC", limit=100)
        
        if not trades:
            client.log_message(
                f"⚠️ {client.sol_wallet.label}: Trade history is empty, cannot calculate average buy price",
                level="WARNING"
            )
            return None, 0
        
        # Фильтруем только покупки (USDC → Token)
        buys = [
            t for t in trades 
            if t.get('from_token') == 'USDC' and t.get('to_token') == token_name
        ]
        
        if not buys:
            client.log_message(
                f"⚠️ {client.sol_wallet.label}: No buy trades found in history",
                level="WARNING"
            )
            return None, 0
        
        # Суммируем покупки от последней, пока не наберем target_amount
        accumulated_tokens = Decimal('0')
        accumulated_usdc = Decimal('0')
        trades_used = 0
        
        for buy in buys:
            to_amount = Decimal(str(buy.get('to_amount', 0)))
            from_amount = Decimal(str(buy.get('from_amount', 0)))
            
            if to_amount <= 0 or from_amount <= 0:
                continue
            
            # Сколько еще нужно набрать
            remaining = target_amount - accumulated_tokens
            
            if remaining <= 0:
                break
            
            # Берем либо всю сделку, либо только часть (если она больше чем нужно)
            if to_amount <= remaining:
                # Берем всю сделку
                accumulated_tokens += to_amount
                accumulated_usdc += from_amount
                trades_used += 1
            else:
                # Берем только часть сделки (пропорционально)
                ratio = remaining / to_amount
                accumulated_tokens += remaining
                accumulated_usdc += from_amount * ratio
                trades_used += 1
                break
        
        if accumulated_tokens > 0:
            avg_price = float(accumulated_usdc / accumulated_tokens)
            client.log_message(
                f"📊 {client.sol_wallet.label}: Average buy price for {accumulated_tokens:.6f}{token_name} (from {trades_used} trades): ${avg_price:.2f}",
                level="INFO"
            )
            return avg_price, trades_used
        
        return None, 0
        
    except Exception as e:
        client.log_message(
            f"⚠️ {client.sol_wallet.label}: Failed to get average buy price from history: {e}",
            level="WARNING"
        )
        return None, 0


async def create_tp_order(client: 'SpotClient', token_name: str, token_amount: Decimal, 
                          tp_price: Decimal, entry_price: Decimal) -> dict:
    """
    Создает TP ордер - размещает лимитный ордер на бирже.
    
    Если ордер не удалось разместить - НЕ добавляет его в список.
    При следующей итерации бот попытается снова.
    
    Args:
        client: SpotClient instance
        token_name: Название токена (например "WBTC")
        token_amount: Количество токена для продажи
        tp_price: Целевая цена Take Profit
        entry_price: Цена входа (для расчета профита)
        
    Returns:
        dict: Информация о созданном TP ордере, или None если не удалось разместить
    """
    # Пытаемся создать лимитный ордер на бирже
    try:
        limit_order = await client.place_limit_order(
            from_token=token_name,
            to_token="USDC",
            amount=token_amount,
            limit_price=float(tp_price)
        )
        
        if limit_order and limit_order.get('order_id'):
            # Успешно создали лимитный ордер на бирже
            tp_order_info = {
                'entry_price': float(entry_price),
                'tp_price': float(tp_price),
                'amount': float(token_amount),
                'timestamp': datetime.now().isoformat(),
                'on_exchange': True,
                'order_id': limit_order['order_id']
            }
            
            # Добавляем в список ТОЛЬКО если успешно разместили на бирже
            client.tp_orders.append(tp_order_info)
            
            client.log_message(
                f"🎯 {client.sol_wallet.label}: Limit order placed on exchange: {token_amount:.6f} {token_name} @ ${tp_price:.2f}",
                level="INFO"
            )
            
            return tp_order_info
        else:
            # API вернул None - ордер НЕ создан
            client.log_message(
                f"❌ {client.sol_wallet.label}: Failed to place limit order: API returned None",
                level="ERROR"
            )
            return None
            
    except Exception as e:
        # Ошибка при создании лимитного ордера
        client.log_message(
            f"❌ {client.sol_wallet.label}: Failed to create limit order: {e}",
            level="ERROR"
        )
        return None


async def get_tp_orders_from_exchange(client: 'SpotClient', token_name: str) -> list:
    """
    Получает список открытых TP ордеров с биржи (фильтрация по status == 0).
    
    API `/api/v1/orders/limit` возвращает все ордера с полем `status`:
    - status == 0 → открытые (pending)
    - status == 1 → исполненные (filled)
    
    Args:
        client: SpotClient instance
        token_name: Название токена (например "WBTC")
        
    Returns:
        list: Список TP ордеров [{order_id, amount, tp_price, entry_price, timestamp}, ...]
    """
    tp_orders = []
    
    try:
        # Получаем открытые лимитные ордера с биржи
        exchange_orders = await client.browser.get_open_limit_orders()
        
        if not exchange_orders:
            return tp_orders
        
        # Получаем адреса токенов и кошелька для фильтрации
        from .config import SOL_TOKEN_ADDRESSES
        input_mint_address = SOL_TOKEN_ADDRESSES.get(token_name)
        output_mint_address = SOL_TOKEN_ADDRESSES.get("USDC")
        user_wallet = str(client.sol_wallet.address)
        
        # Счётчики для диагностики
        status_counts = {}
        filtered_by_status = 0
        filtered_by_tokens = 0
        filtered_by_wallet = 0
        seen_order_ids = set()  # Для проверки на дубликаты
        duplicates_found = 0
        
        # Сохраняем первые 3 ордера со status=0 для анализа
        status_0_samples = []
        
        # Фильтруем только наши ордера (token -> USDC от нашего кошелька)
        for order in exchange_orders:
            input_mint = order.get('input_mint')
            output_mint = order.get('output_mint')
            
            # Проверяем статус ордера (только активные!)
            # API может возвращать статус как int (0, 1, 2) или string
            order_status = order.get('status')
            
            # Подсчитываем статусы для диагностики
            status_key = f"status_{order_status}" if order_status is not None else "status_None"
            status_counts[status_key] = status_counts.get(status_key, 0) + 1
            
            # Сохраняем первые 3 ордера со status=0 для детального анализа
            if order_status == 0 and len(status_0_samples) < 3:
                status_0_samples.append(order)
            
            # ВАЖНО: Проверяем filled_output_amount - API Kamino не всегда обновляет status!
            # Ордер может иметь status=0, но уже быть исполненным (filled_output_amount > 0)
            filled_output = order.get('filled_output_amount')
            filled_input = order.get('filled_input_amount')
            
            if filled_output and filled_output > 0:
                # Ордер уже исполнен, хотя status может быть 0
                filtered_by_status += 1
                continue
            
            if isinstance(order_status, int):
                # Числовые статусы (из API):
                # 0 = pending (активный)
                # 1 = filled (исполнен)
                # 2 = cancelled (отменен)
                if order_status != 0:
                    filtered_by_status += 1
                    continue  # Пропускаем всё кроме pending (0)
            elif isinstance(order_status, str):
                # Строковые статусы
                if order_status.lower() not in ['pending', 'open', 'active', '']:
                    filtered_by_status += 1
                    continue  # Пропускаем cancelled, filled, expired
            # Если статус None или пустой - пропускаем через
            
            # Проверяем принадлежность кошельку (могут быть разные поля)
            order_owner = (
                order.get('user_wallet_address') or 
                order.get('owner') or 
                order.get('user') or
                order.get('wallet_address')
            )
            
            # Фильтрация: правильные токены И наш кошелек И активный статус
            tokens_match = (input_mint == input_mint_address and output_mint == output_mint_address)
            wallet_match = (not order_owner or order_owner == user_wallet)
            
            if not tokens_match:
                filtered_by_tokens += 1
                continue
            
            if not wallet_match:
                filtered_by_wallet += 1
                continue
            
            if tokens_match and wallet_match:
                # Извлекаем данные
                order_id = order.get('limit_order_account_address') or order.get('order_id')
                
                # Проверяем на дубликаты order_id
                if order_id in seen_order_ids:
                    duplicates_found += 1
                    continue
                seen_order_ids.add(order_id)
                
                # Рассчитываем параметры из API ответа
                initial_input_amount = order.get('initial_input_amount', 0)
                input_decimals = order.get('input_mint_decimals', 8)
                expected_output_amount = order.get('expected_output_amount', 0)
                output_decimals = order.get('output_mint_decimals', 6)
                
                # ✅ ФИЛЬТРАЦИЯ ПО STATUS:
                # Теперь фильтруем Ghost orders через status == 0 (открытые)
                # API сам отделяет:
                # - status == 0 → открытые (pending)
                # - status == 1 → исполненные (filled)
                # Ghost orders либо не попадают в API, либо имеют другой status
                # Проверка через Solana RPC больше не нужна!
                
                # Количество токена для продажи
                token_amount = initial_input_amount / (10 ** input_decimals)
                
                # Лимитная цена (USDC за 1 токен)
                usdc_amount = expected_output_amount / (10 ** output_decimals)
                limit_price = usdc_amount / token_amount if token_amount > 0 else 0
                
                # Timestamp
                created_at = order.get('created_at', 0)
                if created_at > 0:
                    timestamp = datetime.fromtimestamp(created_at / 1000).isoformat()
                else:
                    timestamp = datetime.now().isoformat()
                
                # Нормализованный ордер
                tp_orders.append({
                    'order_id': order_id,
                    'limit_order_account_address': order_id,  # Для совместимости
                    'amount': float(token_amount),
                    'tp_price': float(limit_price),
                    'entry_price': float(limit_price - settings.STEP),  # Оценка entry_price
                    'timestamp': timestamp
                })
                
        # Логируем только один раз при старте (с диагностикой)
        if not hasattr(client, '_tp_orders_logged'):
            client._tp_orders_logged = False
        
        if not client._tp_orders_logged:
            client.log_message(
                f"🔍 {client.sol_wallet.label}: Received {len(exchange_orders)} orders from API (before filtering)",
                level="INFO"
            )
            
            # Диагностика фильтрации
            client.log_message(
                f"   📊 Status distribution: {', '.join([f'{k}={v}' for k, v in sorted(status_counts.items())])}",
                level="INFO"
            )
            client.log_message(
                f"   🔻 Filtered out: {filtered_by_status} by status, {filtered_by_tokens} by tokens, {filtered_by_wallet} by wallet, {duplicates_found} duplicates",
                level="INFO"
            )
            
            # Выводим детали первых 3 ордеров со status=0 из API
            client.log_message(
                f"   🔬 API RAW DATA - First 3 orders with status=0:",
                level="INFO"
            )
            import json
            for i, raw_order in enumerate(status_0_samples, 1):
                # Форматируем JSON для читаемости, убираем лишние поля
                relevant_fields = {
                    'order_id': raw_order.get('limit_order_account_address', raw_order.get('order_id', 'N/A'))[:20],
                    'status': raw_order.get('status'),
                    'created_at': raw_order.get('created_at'),
                    'last_updated': raw_order.get('last_updated_timestamp'),
                    'input_mint': raw_order.get('input_mint', '')[:10],
                    'output_mint': raw_order.get('output_mint', '')[:10],
                    'initial_input_amount': raw_order.get('initial_input_amount'),
                    'expected_output_amount': raw_order.get('expected_output_amount'),
                    'filled_input_amount': raw_order.get('filled_input_amount'),
                    'filled_output_amount': raw_order.get('filled_output_amount'),
                    'user_wallet': raw_order.get('user_wallet_address', raw_order.get('owner', ''))[:10]
                }
                client.log_message(
                    f"      Order {i}: {json.dumps(relevant_fields, indent=2)}",
                    level="INFO"
                )
            
            client.log_message(
                f"✅ {client.sol_wallet.label}: Filtered to {len(tp_orders)} active TP orders",
                level="INFO"
            )
            
            if tp_orders:
                # Выводим список всех TP ордеров при старте
                for i, tp in enumerate(sorted(tp_orders, key=lambda x: x['tp_price']), 1):
                    client.log_message(
                        f"   {i}. {tp['amount']:.6f} {token_name} @ ${tp['tp_price']:.2f} (entry: ${tp.get('entry_price', 0):.2f})",
                        level="INFO"
                    )
            
            client._tp_orders_logged = True
            
    except Exception as e:
        client.log_message(
            f"⚠️ {client.sol_wallet.label}: Failed to get TP orders from exchange: {e}",
            level="WARNING"
        )
    
    return tp_orders


async def check_executed_limit_orders(client: 'SpotClient', token_name: str, 
                                      current_tp_orders: list) -> list:
    """
    Проверяет какие лимитные ордера исполнились на бирже.
    
    МЕТОД С ИСПОЛЬЗОВАНИЕМ STATUS:
    - Получаем все ордера с `status == 1` (исполненные)
    - Сравниваем с предыдущим состоянием
    - Находим НОВЫЕ исполненные ордера
    
    Это намного надежнее и быстрее, чем Trade History!
    
    Args:
        client: SpotClient instance
        token_name: Название токена
        current_tp_orders: Текущий список открытых TP ордеров (status == 0)
        
    Returns:
        list: Список НОВЫХ исполненных ордеров
    """
    executed_orders = []
    
    try:
        # Инициализируем кэш предыдущих исполненных ордеров
        if not hasattr(client, '_previous_filled_order_ids'):
            client._previous_filled_order_ids = set()
            client._orders_cache_initialized = False
        
        # Получаем ВСЕ лимитные ордера (открытые + исполненные)
        all_orders = await client.browser.get_open_limit_orders()
        
        if not all_orders:
            return []
        
        # Получаем адреса токенов для фильтрации
        from .config import SOL_TOKEN_ADDRESSES
        input_mint_address = SOL_TOKEN_ADDRESSES.get(token_name)
        output_mint_address = SOL_TOKEN_ADDRESSES.get("USDC")
        
        # Фильтруем только исполненные ордера (status == 1)
        filled_orders = []
        for order in all_orders:
            # Проверяем status
            if order.get('status') != 1:
                continue
            
            # Проверяем токены (WBTC → USDC)
            if (order.get('input_mint') != input_mint_address or 
                order.get('output_mint') != output_mint_address):
                continue
            
            # Это наш исполненный TP ордер!
            order_id = order.get('limit_order_account_address') or order.get('order_id')
            
            # Проверяем возраст ордера (только последние 30 минут)
            current_time = time.time()
            order_timestamp = order.get('last_updated_timestamp', 0) / 1000  # ms → seconds
            order_age_seconds = current_time - order_timestamp
            
            if order_age_seconds > 1800:  # 30 минут = 1800 секунд
                continue  # Пропускаем старые ордера
            
            # При первой инициализации кэша просто добавляем все ордера без обработки
            if not client._orders_cache_initialized:
                client._previous_filled_order_ids.add(order_id)
                continue
            
            # Пропускаем уже обработанные
            if order_id in client._previous_filled_order_ids:
                continue
            
            # НОВЫЙ исполненный ордер!
            # Рассчитываем параметры
            initial_input_amount = order.get('initial_input_amount', 0)
            filled_output_amount = order.get('filled_output_amount', 0)
            input_decimals = order.get('input_mint_decimals', 8)
            output_decimals = order.get('output_mint_decimals', 6)
            
            token_amount = initial_input_amount / (10 ** input_decimals)
            usdc_received = filled_output_amount / (10 ** output_decimals)
            actual_price = usdc_received / token_amount if token_amount > 0 else 0
            
            # Timestamp
            created_at = order.get('created_at', 0)
            if created_at > 0:
                timestamp = datetime.fromtimestamp(created_at / 1000).isoformat()
            else:
                timestamp = datetime.now().isoformat()
            
            # Нормализованный исполненный ордер
            executed_order = {
                'order_id': order_id,
                'amount': float(token_amount),
                'tp_price': float(actual_price),
                'entry_price': float(actual_price - settings.STEP),  # Оценка
                'timestamp': timestamp
            }
            
            executed_orders.append(executed_order)
            
            # Помечаем как обработанный
            client._previous_filled_order_ids.add(order_id)
        
        # Ограничиваем размер кэша (последние 50 ордеров)
        if len(client._previous_filled_order_ids) > 50:
            # Удаляем 25 самых старых
            client._previous_filled_order_ids = set(
                list(client._previous_filled_order_ids)[-25:]
            )
        
        # Помечаем кэш как инициализированный после первого прохода
        if not client._orders_cache_initialized:
            client._orders_cache_initialized = True
                
    except Exception as e:
        client.log_message(
            f"⚠️ {client.sol_wallet.label}: Failed to check executed limit orders: {e}",
            level="WARNING"
        )
    
    return executed_orders




# Кэш для ограничения частоты предупреждающих сообщений
_warning_cache = {}

# Кэш для повторяющихся логов (ограничение: раз в 5 минут)
_repeated_log_cache = {}


def can_log_warning(account_label: str, message_type: str, cooldown_minutes: int = 5) -> bool:
    """
    Проверяет, можно ли выводить предупреждающее сообщение для аккаунта.
    """
    global _warning_cache
    from time import time
    
    current_time = time()
    cache_key = f"{account_label}_{message_type}"
    last_log_time = _warning_cache.get(cache_key, 0)
    cooldown_seconds = cooldown_minutes * 60
    
    if current_time - last_log_time >= cooldown_seconds:
        _warning_cache[cache_key] = current_time
        return True
    
    return False


def calculate_limit_orders_value(current_tp_orders: list) -> float:
    """
    Рассчитывает суммарную стоимость всех активных лимитных ордеров,
    если бы они исполнились по назначенным ценам.
    
    Args:
        current_tp_orders: Список активных TP ордеров
            [{'amount': float, 'tp_price': float, ...}, ...]
    
    Returns:
        float: Суммарная стоимость всех лимитных ордеров в USDC
    """
    if not current_tp_orders:
        return 0.0
    
    total = 0.0
    for order in current_tp_orders:
        amount = order.get('amount', 0)
        tp_price = order.get('tp_price', 0)
        total += amount * tp_price
    
    return total


def format_limit_orders_list(current_tp_orders: list) -> str:
    """
    Форматирует список активных лимитных ордеров в строку,
    отсортированную по возрастанию цены.
    
    Args:
        current_tp_orders: Список активных TP ордеров
            [{'amount': float, 'tp_price': float, ...}, ...]
    
    Returns:
        str: Отформатированный список ордеров, например: "$98000, $99000, $100000" или ""
    """
    if not current_tp_orders:
        return ""
    
    # Сортируем по цене
    sorted_orders = sorted(current_tp_orders, key=lambda x: x.get('tp_price', 0))
    
    # Форматируем список цен
    prices = [f"${order.get('tp_price', 0):.0f}" for order in sorted_orders]
    
    return ", ".join(prices)


async def log_statistics_to_excel(client: SpotClient, operation: str, token_amount: float,
                                  price: float, current_market_price: float, usdc_balance: float, 
                                  token_balance: float, limit_orders_value: float, 
                                  limit_orders_list: str, total_value: float):
    """
    Записывает статистику операции в отдельный Excel файл для каждого аккаунта.
    Формат файла: stat/{account_label}_stat.xlsx
    
    Args:
        current_market_price: Текущая рыночная цена на момент операции (bid/ask)
        price: Цена операции (entry price, tp price, sell price, и т.д.)
    """
    if not settings.ENABLE_EXCEL_STATS:
        return
    
    try:
        import pandas as pd
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Подготавливаем данные для записи (с меткой аккаунта)
        new_row = {
            'Timestamp': timestamp,
            'Account': client.sol_wallet.label,  # Метка аккаунта для идентификации
            'Current Price': current_market_price,  # ← РЕАЛЬНАЯ рыночная цена!
            'Operation': operation,
            'Token Amount': token_amount,
            'Operation Price': price,  # ← Цена операции (entry/tp/sell)
            'USDC Balance': usdc_balance,
            'Token Balance': token_balance,
            'Limit Orders': limit_orders_value,
            'Total Value': total_value,
            'Limit Orders List': limit_orders_list
        }
        
        # Создаем каталог stat, если его нет
        stats_dir = "stat"
        if not os.path.exists(stats_dir):
            os.makedirs(stats_dir)
        
        # Путь к файлу статистики (отдельный файл для каждого аккаунта)
        stats_file = os.path.join(stats_dir, f"{client.sol_wallet.label}_stat.xlsx")
        
        # Проверяем, существует ли файл
        if os.path.exists(stats_file):
            df = pd.read_excel(stats_file)
        else:
            df = pd.DataFrame(columns=[
                'Timestamp', 'Account', 'Current Price', 'Operation', 'Token Amount',
                'Operation Price', 'USDC Balance', 'Token Balance', 'Limit Orders', 
                'Total Value', 'Limit Orders List'
            ])
        
        # Добавляем новую строку
        new_df = pd.DataFrame([new_row])
        df = pd.concat([df, new_df], ignore_index=True)
        
        # Сохраняем в Excel файл
        df.to_excel(stats_file, index=False)
        
        client.log_message(
            f"📊 Statistics logged: {operation} | {token_amount:.6f} @ ${price:.2f}",
            level="DEBUG"
        )
        
    except Exception as e:
        client.log_message(f"Failed to log statistics: {e}", level="WARNING")


async def send_tg_notification(client: SpotClient, text: str, save_to_report: bool = True):
    """
    Отправляет уведомление в Telegram немедленно.
    """
    try:
        await TgReport().send_log(logs=text)
        
        if save_to_report:
            await client.db.append_report(
                key=client.sol_wallet.encoded_pk,
                text=text,
                success=True
            )
    except Exception as e:
        client.log_message(f"Failed to send Telegram notification: {e}", level="DEBUG")


async def calculate_real_profit(client: SpotClient, sold_amount: float, sell_price: float, entry_price: float) -> float:
    """
    Рассчитывает реальную прибыль от сделки
    
    Args:
        client: SpotClient instance
        sold_amount: Количество проданных токенов
        sell_price: Цена продажи
        entry_price: Цена покупки (из TP ордера)
    
    Returns:
        float: Реальная прибыль
    """
    try:
        # Для DEX комиссия минимальная (gas fees), примерно 0.1%
        commission = sold_amount * sell_price * 0.001
        
        # Рассчитываем прибыль
        sell_value = sold_amount * sell_price
        buy_value = sold_amount * entry_price
        real_profit = sell_value - buy_value - commission
        
        client.log_message(
            f"💰 Profit calculation: Sold {sold_amount:.6f} @ ${sell_price:.2f}, "
            f"Bought @ ${entry_price:.2f}, Net profit: ${real_profit:.2f}",
            level="INFO"
        )
        
        return real_profit
        
    except Exception as e:
        client.log_message(f"Failed to calculate real profit: {e}", level="ERROR")
        return 0.0


async def trade_averaging_strategy(client: SpotClient, token_name: str):
    """
    Основная функция стратегии усреднения/пирамидинга
    
    Параметры:
    - client: SpotClient instance
    - token_name: Название токена для торговли (например, "WBTC")
    """
    try:
        # Проверка отключения торговли
        trading_enabled = getattr(settings, 'ENABLE_TRADING', True)
        if not trading_enabled:
            client.log_message(
                f"⚠️ {client.sol_wallet.label}: Trading is DISABLED in settings (ENABLE_TRADING=False)",
                level="WARNING"
            )
            client.log_message(
                f"📊 {client.sol_wallet.label}: Monitoring mode: Only checking TP orders execution, NO NEW TRADES",
                level="INFO"
            )
        
        step = Decimal(str(settings.STEP))
        aggr = Decimal(str(settings.AGGR))
        pwr = step * aggr
        
        client.log_message(
            f"📊 {client.sol_wallet.label}: Starting Averaging Strategy: STEP=${step}, AGGR={aggr}, PWR=${pwr}",
            level="INFO"
        )
        
        # Кэш для отслеживания изменений
        previous_state = None
        orphaned_logged = False  # Флаг для однократного вывода orphaned tokens
        iteration_count = 0  # Счетчик итераций
        last_heartbeat_time = 0  # Время последнего heartbeat
        
        while True:
            # Проверка флага graceful shutdown
            try:
                import main
                if hasattr(main, 'shutdown_requested') and main.shutdown_requested:
                    client.log_message(
                        f"🛑 {client.sol_wallet.label}: Graceful shutdown requested. Stopping strategy...",
                        level="WARNING"
                    )
                    return True
            except:
                pass  # Если не удалось импортировать - продолжаем
            
            try:
                # Получаем текущие TP ордера с биржи (источник истины!)
                current_tp_orders = await get_tp_orders_from_exchange(client, token_name)
                
                # Рассчитываем стоимость лимитных ордеров (один раз для всей итерации)
                limit_orders_value = calculate_limit_orders_value(current_tp_orders)
                limit_orders_list = format_limit_orders_list(current_tp_orders)
                
                # Получаем текущую цену
                current_price = await client.get_current_price(token_name)
                
                # Получаем балансы
                usdc_balance = await client.get_usdc_balance()
                token_balance = await client.get_token_balance(token_name)
                
                # Рассчитываем общую стоимость (включая лимитные ордера)
                total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                
                # Формируем текущее состояние для проверки изменений
                current_state = {
                    'price': float(current_price),
                    'usdc': float(usdc_balance),
                    'token': float(token_balance),
                    'tp_orders_count': len(current_tp_orders)
                }
                
                # Собираем информацию о балансах для объединенного стартового сообщения
                if iteration_count == 0:
                    global _startup_balances, _startup_message_sent
                    
                    async with _startup_lock:
                        # Добавляем информацию о текущем аккаунте
                        _startup_balances[client.sol_wallet.label] = {
                            'usdc': float(usdc_balance),
                            'token': float(token_balance),
                            'token_name': token_name,
                            'limit_orders': limit_orders_value,
                            'total': total_value,
                            'client': client
                        }
                        
                        # Если это первый аккаунт, запускаем задачу отправки сообщения
                        if len(_startup_balances) == 1:
                            asyncio.create_task(send_combined_startup_message())
                
                # НЕ логируем состояние каждую итерацию - только события!
                previous_state = current_state.copy()
                
                # Проверяем исполненные лимитные ордера (сравнение состояний)
                # НЕ проверяем при первом запуске (iteration_count == 0), чтобы избежать дублирования старых сообщений
                if iteration_count > 0:
                    executed_orders = await check_executed_limit_orders(client, token_name, current_tp_orders)
                else:
                    executed_orders = []
                
                # Обрабатываем исполненные ордера
                for i, executed_order in enumerate(executed_orders):
                    try:
                        # Получаем данные об исполненном ордере
                        tp_price = float(executed_order['tp_price'])
                        amount = float(executed_order['amount'])
                        entry_price = float(executed_order.get('entry_price', tp_price - settings.STEP))
                        
                        # Рассчитываем реальную прибыль
                        real_profit = await calculate_real_profit(
                            client,
                            amount,
                            tp_price,
                            entry_price
                        )
                        
                        # 📊 ОБНОВЛЯЕМ БАЛАНСЫ ПЕРЕД записью текущего TP
                        # Для старых TP (при запуске) балансы могут быть одинаковые из-за кэша API
                        # Для новых TP (в реальном времени) балансы будут актуальные
                        usdc_balance = await client.get_usdc_balance()
                        token_balance = await client.get_token_balance(token_name)
                        # Пересчитываем limit_orders после исполнения TP
                        current_tp_orders = await get_tp_orders_from_exchange(client, token_name)
                        limit_orders_value = calculate_limit_orders_value(current_tp_orders)
                        limit_orders_list = format_limit_orders_list(current_tp_orders)
                        total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                        
                        # Логируем прибыль
                        client.log_message(
                            f"✅ Profit ${real_profit:.2f} | "
                            f"${usdc_balance:.2f}USDC + {token_balance:.6f}{token_name} = ${total_value:.2f}",
                            level="INFO"
                        )
                        
                        # Формируем сообщение о профите
                        if limit_orders_value > 0:
                            profit_message = (
                                f"🎯 <b>{client.sol_wallet.label}: Profit ${real_profit:.2f}</b> | "
                                f"${usdc_balance:.2f}USDC + {token_balance:.6f}{token_name} + ${limit_orders_value:.0f} Limit Orders = ${total_value:.2f}"
                            )
                        else:
                            profit_message = (
                                f"🎯 <b>{client.sol_wallet.label}: Profit ${real_profit:.2f}</b> | "
                                f"${usdc_balance:.2f}USDC + {token_balance:.6f}{token_name} = ${total_value:.2f}"
                            )
                        
                        # Отправляем в ОСНОВНОЙ бот
                        await send_tg_notification(client, profit_message, save_to_report=False)
                        
                        # ДУБЛИРУЕМ в PROFIT бот
                        await send_profit_notification(profit_message)
                        
                        # Записываем статистику с ТЕКУЩИМ балансом
                        await log_statistics_to_excel(
                            client=client,
                            operation="Take Profit",
                            token_amount=amount,
                            price=tp_price,
                            current_market_price=float(current_price),
                            usdc_balance=float(usdc_balance),
                            token_balance=float(token_balance),
                            limit_orders_value=limit_orders_value,
                            limit_orders_list=limit_orders_list,
                            total_value=total_value
                        )
                        
                        # ⏱️ Небольшая задержка между TP для обновления баланса API
                        # (только если это не последний TP в списке)
                        if i < len(executed_orders) - 1:
                            await async_sleep(0.5)
                        
                    except Exception as e:
                        client.log_message(f"{client.sol_wallet.label}: Failed to process executed TP: {e}", level="ERROR")
                
                # Рассчитываем min и max TP цены из ордеров на бирже
                min_tp_price = None
                max_tp_price = None
                
                if current_tp_orders:
                    tp_prices = [Decimal(str(tp['tp_price'])) for tp in current_tp_orders]
                    min_tp_price = min(tp_prices)
                    max_tp_price = max(tp_prices)
                
                # Рассчитываем размер позиции
                position_size = await client.calculate_position_size()
                
                # Проверяем, покрывают ли TP ордера весь баланс токенов
                total_tp_amount = sum(Decimal(str(tp['amount'])) for tp in current_tp_orders) if current_tp_orders else Decimal('0')
                orphaned_amount = token_balance - total_tp_amount
                
                # 1. Информация об orphaned tokens (без автоматического создания TP)
                # Выводится только один раз при запуске
                if orphaned_amount > Decimal('0.00001') and not orphaned_logged:
                    # Получаем среднюю цену покупки для информации
                    avg_buy_price, trades_count = await get_average_buy_price_for_amount(
                        client, token_name, orphaned_amount
                    )
                    
                    if avg_buy_price and avg_buy_price > 0:
                        entry_price_estimate = Decimal(str(avg_buy_price))
                        client.log_message(
                            f"ℹ️ {client.sol_wallet.label}: Orphaned tokens: {orphaned_amount:.6f}{token_name} | Avg buy price: ${entry_price_estimate:.0f} (from {trades_count} trades)",
                            level="INFO"
                        )
                    else:
                        client.log_message(
                            f"ℹ️ {client.sol_wallet.label}: Orphaned tokens: {orphaned_amount:.6f}{token_name} | No trade history available",
                            level="INFO"
                        )
                    
                    # Отмечаем что уже вывели информацию
                    orphaned_logged = True
                
                # 2. Первая позиция (если нет TP ордеров)
                # Как в hype: если нет TP ордеров → First Position (даже если есть токены на балансе)
                if not current_tp_orders:
                    
                    # Проверка: торговля включена?
                    if not trading_enabled:
                        await async_sleep(10)
                        continue
                    
                    # Нет TP ордеров - создаем первую позицию
                    # Ограничиваем вывод: не чаще раза в 5 минут
                    if can_log_warning(client.label, "no_tp_orders"):
                        client.log_message(
                            f"🚀 {client.sol_wallet.label}: No TP Orders: creating MARKET BUY at ${current_price:.0f}",
                            level="INFO"
                        )
                    
                    # Проверяем достаточность средств
                    if usdc_balance < position_size:
                        if can_log_warning(client.label, "insufficient_balance_first"):
                            client.log_message(
                                f"⚠️ {client.sol_wallet.label}: Insufficient USDC balance: ${usdc_balance:.2f} < ${position_size:.2f}",
                                level="WARNING"
                            )
                        await async_sleep(10)
                        continue
                    
                    try:
                        # Выполняем покупку
                        buy_result = await client.place_market_order(
                            from_token="USDC",
                            to_token=token_name,
                            amount=position_size
                        )
                        
                        if buy_result:
                            actual_price = Decimal(str(buy_result['price']))
                            token_amount = Decimal(str(buy_result['to_amount']))
                            usdc_spent = float(buy_result['from_amount'])
                            
                            # Логируем покупку
                            client.log_message(
                                f"Open long market order {token_amount:.5f} {token_name} at {actual_price:.0f} ({usdc_spent:.2f}$)",
                                level="INFO"
                            )
                            
                            # Получаем актуальный баланс после покупки
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Записываем статистику First Position СНАЧАЛА
                            await log_statistics_to_excel(
                                client=client,
                                operation="First Position",
                                token_amount=float(token_amount),
                                price=float(actual_price),
                                current_market_price=float(current_price),
                                usdc_balance=float(usdc_balance),
                                token_balance=float(token_balance),
                                limit_orders_value=limit_orders_value,
                                limit_orders_list=limit_orders_list,
                                total_value=total_value
                            )
                            
                            # Создаем TP ордер (лимитный на бирже)
                            tp_price = actual_price + step
                            tp_order = await create_tp_order(
                                client=client,
                                token_name=token_name,
                                token_amount=token_amount,
                                tp_price=float(tp_price),
                                entry_price=float(actual_price)
                            )
                            
                            # Получаем актуальный баланс после создания TP (может измениться из-за комиссий)
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            # После создания нового TP ордера, пересчитываем список TP ордеров
                            current_tp_orders = await get_tp_orders_from_exchange(client, token_name)
                            limit_orders_value = calculate_limit_orders_value(current_tp_orders)
                            limit_orders_list = format_limit_orders_list(current_tp_orders)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Логируем TP
                            if tp_order:
                                client.log_message(
                                    f"{client.sol_wallet.label}: set TP: {token_amount:.5f} @ ${actual_price:.0f} → ${tp_price:.0f}",
                                    level="INFO"
                                )
                                
                                # Записываем статистику Set TP ПОСЛЕ
                                await log_statistics_to_excel(
                                    client=client,
                                    operation="Set TP",
                                    token_amount=float(token_amount),
                                    price=float(tp_price),
                                    current_market_price=float(current_price),
                                    usdc_balance=float(usdc_balance),
                                    token_balance=float(token_balance),
                                    limit_orders_value=limit_orders_value,
                                    limit_orders_list=limit_orders_list,
                                    total_value=total_value
                                )
                            else:
                                if can_log_warning(client.label, "tp_order_failed"):
                                    client.log_message(
                                        f"{client.sol_wallet.label}: ⚠️ TP order failed, will retry next iteration",
                                        level="WARNING"
                                    )
                            
                            # Отправляем уведомление
                            await send_tg_notification(
                                client,
                                f"🚀 <b>{client.sol_wallet.label}: First Position</b>\n"
                                f"BUY {token_amount:.6f}{token_name} @ ${actual_price:.2f}\n"
                                f"🎯 TP: ${tp_price:.2f}",
                                save_to_report=False
                            )
                            
                    except Exception as e:
                        client.log_message(f"{client.sol_wallet.label}: Failed to create first position: {e}", level="ERROR")
                
                # 3. Усреднение (если цена упала) - ОТДЕЛЬНАЯ проверка!
                if min_tp_price and current_price < (min_tp_price - step * 2):
                    trigger_level = min_tp_price - step * 2
                    
                    # Проверка: торговля включена?
                    if not trading_enabled:
                        await async_sleep(10)
                        continue
                    
                    client.log_message(
                        f"💸 {client.sol_wallet.label}: Averaging: ${current_price:.0f} < ${min_tp_price:.0f} - ${step:.0f}×2 = ${trigger_level:.0f}",
                        level="INFO"
                    )
                    
                    # Проверяем достаточность средств
                    if usdc_balance < position_size:
                        if can_log_warning(client.label, "insufficient_balance_averaging"):
                            client.log_message(
                                f"⚠️ {client.sol_wallet.label}: Insufficient USDC for averaging: ${usdc_balance:.2f} < ${position_size:.2f}",
                                level="WARNING"
                            )
                        await async_sleep(10)
                        continue
                    
                    try:
                        # Выполняем покупку
                        buy_result = await client.place_market_order(
                            from_token="USDC",
                            to_token=token_name,
                            amount=position_size
                        )
                        
                        if buy_result:
                            actual_price = Decimal(str(buy_result['price']))
                            token_amount = Decimal(str(buy_result['to_amount']))
                            usdc_spent = float(buy_result['from_amount'])
                            
                            # Логируем покупку
                            client.log_message(
                                f"Open long market order {token_amount:.5f} {token_name} at {actual_price:.0f} ({usdc_spent:.2f}$)",
                                level="INFO"
                            )
                            
                            # Получаем актуальный баланс после покупки
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Записываем статистику Averaging СНАЧАЛА
                            await log_statistics_to_excel(
                                client=client,
                                operation="Averaging",
                                token_amount=float(token_amount),
                                price=float(actual_price),
                                current_market_price=float(current_price),
                                usdc_balance=float(usdc_balance),
                                token_balance=float(token_balance),
                                limit_orders_value=limit_orders_value,
                                limit_orders_list=limit_orders_list,
                                total_value=total_value
                            )
                            
                            # Создаем TP ордер (лимитный на бирже)
                            tp_price = actual_price + step
                            tp_order = await create_tp_order(
                                client=client,
                                token_name=token_name,
                                token_amount=token_amount,
                                tp_price=tp_price,
                                entry_price=actual_price
                            )
                            
                            # Получаем актуальный баланс после создания TP
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            # После создания нового TP ордера, пересчитываем список TP ордеров
                            current_tp_orders = await get_tp_orders_from_exchange(client, token_name)
                            limit_orders_value = calculate_limit_orders_value(current_tp_orders)
                            limit_orders_list = format_limit_orders_list(current_tp_orders)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Логируем TP
                            if tp_order:
                                client.log_message(
                                    f"{client.sol_wallet.label}: set TP: {token_amount:.5f} @ ${actual_price:.0f} → ${tp_price:.0f}",
                                    level="INFO"
                                )
                                
                                # Записываем статистику Set TP ПОСЛЕ
                                await log_statistics_to_excel(
                                    client=client,
                                    operation="Set TP",
                                    token_amount=float(token_amount),
                                    price=float(tp_price),
                                    current_market_price=float(current_price),
                                    usdc_balance=float(usdc_balance),
                                    token_balance=float(token_balance),
                                    limit_orders_value=limit_orders_value,
                                    limit_orders_list=limit_orders_list,
                                    total_value=total_value
                                )
                            else:
                                if can_log_warning(client.label, "tp_order_failed_averaging"):
                                    client.log_message(
                                        f"{client.sol_wallet.label}: ⚠️ TP order failed for averaging",
                                        level="WARNING"
                                    )
                            
                            # Отправляем уведомление
                            await send_tg_notification(
                                client,
                                f"📉 <b>{client.sol_wallet.label}: Averaging</b>\n"
                                f"BUY {token_amount:.6f}{token_name} @ ${actual_price:.2f}\n"
                                f"🎯 TP: ${tp_price:.2f}",
                                save_to_report=False
                            )
                            
                    except Exception as e:
                        client.log_message(f"{client.sol_wallet.label}: Failed to execute averaging: {e}", level="ERROR")
                
                # 4. Пирамидинг (если цена растет) - ОТДЕЛЬНАЯ проверка!
                if max_tp_price and current_price > (max_tp_price - pwr):
                    trigger_level = max_tp_price - pwr
                    
                    # Проверка: торговля включена?
                    if not trading_enabled:
                        await async_sleep(10)
                        continue
                    
                    client.log_message(
                        f"📈 {client.sol_wallet.label}: Pyramiding: ${current_price:.0f} > ${max_tp_price:.0f} - ${pwr:.0f} = ${trigger_level:.0f}",
                        level="INFO"
                    )
                    
                    # Проверяем достаточность средств
                    if usdc_balance < position_size:
                        if can_log_warning(client.label, "insufficient_balance_pyramiding"):
                            client.log_message(
                                f"⚠️ {client.sol_wallet.label}: Insufficient USDC for pyramiding: ${usdc_balance:.2f} < ${position_size:.2f}",
                                level="WARNING"
                            )
                        await async_sleep(10)
                        continue
                    
                    try:
                        # Выполняем покупку
                        buy_result = await client.place_market_order(
                            from_token="USDC",
                            to_token=token_name,
                            amount=position_size
                        )
                        
                        if buy_result:
                            actual_price = Decimal(str(buy_result['price']))
                            token_amount = Decimal(str(buy_result['to_amount']))
                            usdc_spent = float(buy_result['from_amount'])
                            
                            # Логируем покупку
                            client.log_message(
                                f"Open long market order {token_amount:.5f} {token_name} at {actual_price:.0f} ({usdc_spent:.2f}$)",
                                level="INFO"
                            )
                            
                            # Получаем актуальный баланс после покупки
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Записываем статистику Pyramiding СНАЧАЛА
                            await log_statistics_to_excel(
                                client=client,
                                operation="Pyramiding",
                                token_amount=float(token_amount),
                                price=float(actual_price),
                                current_market_price=float(current_price),
                                usdc_balance=float(usdc_balance),
                                token_balance=float(token_balance),
                                limit_orders_value=limit_orders_value,
                                limit_orders_list=limit_orders_list,
                                total_value=total_value
                            )
                            
                            # Создаем TP ордер (лимитный на бирже)
                            tp_price = actual_price + step
                            tp_order = await create_tp_order(
                                client=client,
                                token_name=token_name,
                                token_amount=token_amount,
                                tp_price=tp_price,
                                entry_price=actual_price
                            )
                            
                            # Получаем актуальный баланс после создания TP
                            usdc_balance = await client.get_usdc_balance()
                            token_balance = await client.get_token_balance(token_name)
                            # После создания нового TP ордера, пересчитываем список TP ордеров
                            current_tp_orders = await get_tp_orders_from_exchange(client, token_name)
                            limit_orders_value = calculate_limit_orders_value(current_tp_orders)
                            limit_orders_list = format_limit_orders_list(current_tp_orders)
                            total_value = float(usdc_balance) + (float(token_balance) * float(current_price)) + limit_orders_value
                            
                            # Логируем TP
                            if tp_order:
                                client.log_message(
                                    f"{client.sol_wallet.label}: set TP: {token_amount:.5f} @ ${actual_price:.0f} → ${tp_price:.0f}",
                                    level="INFO"
                                )
                                
                                # Записываем статистику Set TP ПОСЛЕ
                                await log_statistics_to_excel(
                                    client=client,
                                    operation="Set TP",
                                    token_amount=float(token_amount),
                                    price=float(tp_price),
                                    current_market_price=float(current_price),
                                    usdc_balance=float(usdc_balance),
                                    token_balance=float(token_balance),
                                    limit_orders_value=limit_orders_value,
                                    limit_orders_list=limit_orders_list,
                                    total_value=total_value
                                )
                            else:
                                if can_log_warning(client.label, "tp_order_failed_pyramiding"):
                                    client.log_message(
                                        f"{client.sol_wallet.label}: ⚠️ TP order failed for pyramiding",
                                        level="WARNING"
                                    )
                            
                            # Отправляем уведомление
                            await send_tg_notification(
                                client,
                                f"📈 <b>{client.sol_wallet.label}: Pyramiding</b>\n"
                                f"BUY {token_amount:.6f}{token_name} @ ${actual_price:.2f}\n"
                                f"🎯 TP: ${tp_price:.2f}",
                                save_to_report=False
                            )
                            
                    except Exception as e:
                        client.log_message(f"{client.sol_wallet.label}: Failed to execute pyramiding: {e}", level="ERROR")
                
                # Heartbeat - признаки жизни (раз в 10 минут)
                import time
                current_time = time.time()
                if current_time - last_heartbeat_time >= 600:  # 10 минут = 600 секунд
                    if limit_orders_value > 0:
                        client.log_message(
                            f"💚 {client.sol_wallet.label}: Active | Price: ${current_price:.0f} | TPs: {len(current_tp_orders)} | "
                            f"Balance: ${usdc_balance:.2f} + {token_balance:.6f} {token_name} + ${limit_orders_value:.0f} Limit Orders = ${total_value:.2f}",
                            level="INFO"
                        )
                    else:
                        client.log_message(
                            f"💚 {client.sol_wallet.label}: Active | Price: ${current_price:.0f} | TPs: {len(current_tp_orders)} | "
                            f"Balance: ${usdc_balance:.2f} + {token_balance:.6f} {token_name} = ${total_value:.2f}",
                            level="INFO"
                        )
                    last_heartbeat_time = current_time
                
                # Увеличиваем счётчик итераций в конце успешной обработки
                iteration_count += 1
                
                # Ждем перед следующей итерацией
                await async_sleep(10)
                
            except Exception as e:
                client.log_message(f"❌ {client.sol_wallet.label}: Trading error: {e}", level="ERROR")
                import traceback
                logger.error(f"Full traceback:\n{traceback.format_exc()}")
                await async_sleep(5)
                
    except Exception as e:
        client.log_message(f"❌ {client.sol_wallet.label}: CRITICAL Strategy error: {e}", level="ERROR")
        import traceback
        logger.error(f"CRITICAL ERROR - Full traceback:\n{traceback.format_exc()}")
        await send_warning_notification(
            error_type="Strategy Critical Error",
            error_message=str(e),
            account_label=client.label
        )
        # НЕ завершаем стратегию! Ждем и пробуем снова
        logger.warning(f"[⚠️] {client.sol_wallet.label}: Strategy encountered critical error, restarting in 30 seconds...")
        await async_sleep(30)
        
        # Рекурсивно перезапускаем стратегию
        return await trade_averaging_strategy(client, token_name)
    
    return True

