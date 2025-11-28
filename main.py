from warnings import filterwarnings
from random import randint, choice
from os import name as os_name
from loguru import logger
from time import sleep
import asyncio
import signal

from modules.utils import choose_mode
from modules.retry import DataBaseError
from modules import *
import settings


# Глобальная переменная для отслеживания сигналов завершения
shutdown_requested = False


def signal_handler(signum, frame):
    """
    Обработчик сигналов SIGINT (Ctrl+C) и SIGTERM
    """
    global shutdown_requested
    sig_name = signal.Signals(signum).name
    
    # Логируем источник сигнала для диагностики
    import traceback
    import os
    logger.warning(f'\n[⚠️] Received signal {sig_name} ({signum})')
    logger.warning(f'[⚠️] Process ID: {os.getpid()}')
    logger.warning(f'[⚠️] Signal source traceback:')
    for line in traceback.format_stack(frame):
        logger.warning(line.strip())
    
    if shutdown_requested:
        logger.error('[!] Force shutdown requested! Exiting immediately...')
        exit(1)
    
    shutdown_requested = True
    logger.warning('[⚠️] Graceful shutdown initiated. Press Ctrl+C again to force exit.')
    logger.info('[•] Waiting for current operations to complete...')


async def aclose_session(browser: Browser, sol_wallet: SolWallet):
    try:
        await browser.session.close()
        await sol_wallet.client.close()

    except Exception as err:
        logger.error(f'[-] Soft | {sol_wallet.address} | FAILED TO CLOSE SESSIONS: {err}')


async def run_module(mode: int, module_data: dict, sem: asyncio.Semaphore):
    async with address_locks[module_data["sol_address"]]:
        async with sem:
            browser = None
            sol_wallet = None
            
            try:
                browser = Browser(
                    db=db,
                    proxy=module_data["proxy"],
                    sol_address=module_data["sol_address"]
                )

                sol_wallet = SolWallet(
                    privatekey=module_data["sol_pk"],
                    encoded_pk=module_data["sol_encoded_pk"],
                    label=module_data["label"],
                    browser=browser,
                    db=db,
                )

                module_data["module_info"]["status"] = await Ranger(sol_wallet=sol_wallet).start(mode=mode)

            except DataBaseError:
                module_data = None
                raise

            except Exception as err:
                if sol_wallet:
                    logger.error(f'[-] Soft | {sol_wallet.address} | Account error | "{err.__class__.__name__}": {err}')
                    await db.append_report(key=sol_wallet.encoded_pk, text=str(err), success=False)
                else:
                    logger.error(f'[-] Soft | {module_data["label"]} | Initialization error | "{err.__class__.__name__}": {err}')
                    await db.append_report(key=module_data["sol_encoded_pk"], text=str(err), success=False)

            finally:
                if type(module_data) == dict:
                    if browser and sol_wallet:
                        await aclose_session(browser, sol_wallet)
                    elif browser:
                        try:
                            await browser.session.close()
                        except:
                            pass
                    
                    await db.remove_module(module_data=module_data)

                    if sol_wallet:
                        reports = await db.get_account_reports(sol_encoded_pk=sol_wallet.encoded_pk, mode=mode)
                        # Не отправляем пустые отчёты "No actions"
                        if reports and "No actions" not in reports:
                            await TgReport().send_log(logs=reports)

                    if module_data["module_info"]["status"] is True:
                        await async_sleep(randint(*settings.SLEEP_AFTER_ACC))
                    else: await async_sleep(10)


async def runner(mode: int):
    all_modules = db.get_all_modules()
    
    if all_modules != 'No more accounts left':
        # Подсчитываем уникальные аккаунты (по sol_address)
        unique_accounts = len(set(module_data['sol_address'] for module_data in all_modules))
        
        # Автоматическое определение количества потоков
        if settings.THREADS <= 0:
            # 0 или отрицательное = автоматически по количеству аккаунтов
            threads = unique_accounts
            logger.info(f'[•] Soft | Auto-detected {unique_accounts} accounts ({len(all_modules)} modules) → using {threads} threads')
        else:
            # Используем заданное значение, но не больше количества аккаунтов
            threads = min(settings.THREADS, unique_accounts)
            if threads < settings.THREADS:
                logger.info(f'[•] Soft | Using {threads} threads (limited by {unique_accounts} accounts)')
            else:
                logger.info(f'[•] Soft | Using {threads} threads (as configured)')
        
        sem = asyncio.Semaphore(threads)
        
        await asyncio.gather(*[
            run_module(
                mode=mode,
                module_data=module_data,
                sem=sem,
            )
            for module_data in all_modules
        ])

    logger.success(f'All accounts done.')
    return 'Ended'


if __name__ == '__main__':
    filterwarnings("ignore")
    if os_name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info('[•] Signal handlers registered (SIGINT, SIGTERM)')

    try:
        db = DataBase()

        while True:
            mode = choose_mode()

            match mode.type:
                case "database":
                    db.create_modules()

                case "module":
                    if asyncio.run(runner(mode=mode.soft_id)) == 'Ended': break
                    print('')

        sleep(0.1)
        input('\n > Exit\n')

    except KeyboardInterrupt:
        pass

    except DataBaseError as err:
        logger.error(f'[-] Database | {err}')

    finally:
        logger.info('[•] Soft | Closed')
