#!/usr/bin/python3

import asyncio
import sys
import logging
import datetime
import argparse
from contextlib import suppress
import aiohttp
from bs4 import BeautifulSoup


async def parse_events(html, queue, share_state_d):

    logger = logging.getLogger('parse_events')

    soup = BeautifulSoup(html, 'html5lib')
    table = soup.find('table')

    if not table:
        logger.error('Unable to find table element in HTML')
        return

    for row in table.findAll('tr')[1:][::-1]:
        col = row.findAll('td')
        coin = col[2].getText()
        coin_amount = float(coin. split(' ')[0])
        coin_name = ' '.join(coin. split(' ')[1:])
        time = col[5].find('span').attrs['title']
        dt = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
        # Seems YIIMP first adds the block withtout the coin amount
        # Next iteration get the proper amount
        if dt > share_state_d['previous_poll_dt'] and coin_amount != 0:
            logger.info('New event found while parsing: %f %s found at %s', coin_amount, coin_name, dt)
            queue.put_nowait((dt, coin_name.upper(), coin_amount))
            share_state_d['previous_poll_dt'] = dt


async def poll_yiimp_events(url, queue):

    logger = logging.getLogger('poll_yiimp_events')
    # Use a dict here, easier to keep reference by updating
    share_state_d = dict()
    share_state_d['previous_poll_dt'] = datetime.datetime.utcnow()
    # For testing purpose
    #share_state_d['previous_poll_dt'] = share_state_d['previous_poll_dt'] - datetime.timedelta(minutes=240)

    # Give other coroutines enought time to refresh markets prices
    with suppress(asyncio.CancelledError):
        await asyncio.sleep(5)

    try:
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as resp:
                        resp.raise_for_status()
                        html = await resp.text()
                await parse_events(html, queue, share_state_d)
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception('Exception occurred: %s: %s', e.__class__.__name__, e)
                await asyncio.sleep(5)

    except asyncio.CancelledError:
        logger.info('Exiting on cancel signal')
        return


async def post_events_discord(url, queue):

    logger = logging.getLogger('post_events_discord')

    try:
        while True:

            try:
                dt, coin_name, coin_amount = await queue.get()

                message = ' :boom: https://Altcoinminers.us/ dug up %f %s found at %s EST :boom: ' % (coin_amount, coin_name, dt)

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json={ 'content': message }) as resp:
                        resp.raise_for_status()

                logger.info('Message "%s" successfully posted to Discord', message)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception('Exception occurred: %s: %s', e.__class__.__name__, e)

    except asyncio.CancelledError:
        logger.info('Exiting on cancel signal')
        return


if __name__ == '__main__':

    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    logging.basicConfig(level=logging.INFO, format='%(levelname)-8s [%(name)-25s] %(message)s', stream=sys.stdout)
    logger = logging.getLogger('main')


    def cli_arguments():
        parser = argparse.ArgumentParser(description='Service polling YIIMP pool to output found blocks on Discord (with currency conversion)')
        parser.add_argument('-p', '--pool-url',    required=True, type=str, help='YIIMP pool url to block found page',     metavar='http://yiimp.eu/site/found_results')
        parser.add_argument('-d', '--discord-url', required=True, type=str, help='Discord webhook url to publish message', metavar='https://discordapp.com/api/webhooks/123456/abdHD76Hhdhngga')
        return parser.parse_args()

    config = cli_arguments()

    pool_url = config.pool_url
    discord_url = config.discord_url

    try:

        logger.info('Starting poll_yiimp_events coroutine')
        poll_yiimp_events_coro = loop.create_task(poll_yiimp_events(pool_url, queue))

        logger.info('Starting post_events_discord coroutine')
        post_events_discord_coro = loop.create_task(post_events_discord(discord_url, queue))

        loop.run_until_complete(asyncio.gather(poll_yiimp_events_coro,
                                               post_events_discord_coro,
                                              ))

    except KeyboardInterrupt:
        all_tasks = asyncio.Task.all_tasks()
        logger.info('Exit signal received, stopping %d coroutines', len(all_tasks))
        for task in all_tasks:
            task.cancel()
        loop.run_until_complete(asyncio.wait_for(asyncio.gather(*all_tasks), timeout=10))

    loop.close()
