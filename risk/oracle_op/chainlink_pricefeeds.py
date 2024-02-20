import os
import json
import logging
import pandas_gbq
import pandas as pd
from web3 import Web3
from web3.exceptions import (InvalidEventABI, LogTopicError, MismatchedABI)
from web3._utils.events import get_event_data
from eth_utils import (encode_hex, event_abi_to_log_topic)
from google.oauth2 import service_account



# The dafault RPC from ethersjs, change it if it doesn't work: https://infura.io/docs
RPC_Endpoint = 'https://mainnet.infura.io/v3/84842078b09946638c03157f83405213'

abi = json.loads("""[
                     {"name":"decimals",
                      "inputs":[],"outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view",
                      "type":"function"},
                     {"name":"AnswerUpdated",
                      "anonymous":false,
                      "inputs":[{"indexed":true,"internalType":"int256","name":"current","type":"int256"},{"indexed":true,"internalType":"uint256","name":"roundId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"updatedAt","type":"uint256"}],
                      "type":"event"}
                    ]
                """)



def get_event_abi(abi, abi_name, abi_type = 'event'):
    l = [x for x in abi if x['type'] == abi_type and x['name']==abi_name]
    return l[0]
def get_logs (w3, contract_address, topics, events, from_block=0, to_block=None):

    if not to_block:
        to_block = w3.eth.get_block('latest').number

    try:
        logs = w3.eth.get_logs({"address": contract_address
                                ,"topics":topics
                                ,"fromBlock": from_block
                                ,"toBlock": to_block
                                })
    except ValueError:
        logs = None
        from_block = 12*10**6
        batch_size =  3*10**5
        while from_block < to_block:
            logging.info(f'from_block {from_block} to_block {from_block + batch_size}')
            batch_logs = w3.eth.get_logs({"address": contract_address
                                    ,"topics":topics
                                    ,"fromBlock": from_block
                                    ,"toBlock": from_block + batch_size # narrow down the range to avoid too many results error
                                    })
            if not logs:
                logs = batch_logs
            else:
                logs = logs + batch_logs
            if len(logs) > 0:
                logging.info(f'Total logs count {len(logs)}')

            from_block = from_block + batch_size + 1

    all_events = []
    for l in logs:
        try:
            evt_topic0 = l['topics'][0].hex()
            evt_abi = [x for x in events if encode_hex(event_abi_to_log_topic(x)) == evt_topic0][0]
            evt = get_event_data(w3.codec, evt_abi, l)
        except MismatchedABI: #if for some reason there are other events
            pass
        all_events.append(evt)
    df = pd.DataFrame(all_events)
    return df

def main():
    gcp_project_id = 'gearbox-336415'
    from_block = 0
    try:
        df_blocknum = pandas_gbq.read_gbq('select max(blockNumber) from gearbox.oracle_price_history',
                                 project_id=gcp_project_id,
                                 progress_bar_type = None,)
        from_block = int(df_blocknum.iloc[0,0]) + 1
    except pandas_gbq.exceptions.GenericGBQException:
        logging.info('The table does not exist?')
        from_block = 0

    #from_block = 0
    logging.info(f'from_block={from_block}')

    df_columns = ['ticker', 'updated_at', 'chain','price', 'price_decimal', 'type', 'threshold', 'base', 'heartbeat', 'decimals', 'blockNumber','tx_hash']
    df = pd.DataFrame(columns = df_columns)
    df_eth_usd = pd.DataFrame(columns = df_columns)

    event_AnswerUpdated = get_event_abi(abi, 'AnswerUpdated')
    topic_AnswerUpdated  = encode_hex(event_abi_to_log_topic(event_AnswerUpdated))

    for chain in config.keys():
        chain_contracts = config[chain]
        for ticker in chain_contracts.keys():
            w3 = w3_connection[chain_contracts[ticker][0]]
            address = chain_contracts[ticker][1]
            threshold = chain_contracts[ticker][2]
            base = chain_contracts[ticker][3]
            heartbeat = chain_contracts[ticker][4]

            decimals = w3.eth.contract(address=address, abi=abi).functions.decimals().call()

            df_ticker = get_logs(w3, address, [topic_AnswerUpdated], [event_AnswerUpdated], from_block)
            if len(df_ticker)>0:
                df_ticker[['chain', 'ticker', 'threshold', 'base', 'heartbeat', 'decimals', 'type']] = [chain , ticker, threshold, base, heartbeat, decimals, 'direct']

                df_ticker['tx_hash']       = df_ticker['transactionHash'].apply(lambda x: x.hex())
                df_ticker['updated_at']    = df_ticker['args'].apply(lambda x: pd.to_datetime(x['updatedAt'], unit='s'))
                df_ticker['price']         = df_ticker['args'].apply(lambda x: x['current'])
                df_ticker['price_decimal'] = df_ticker['price']/10**(decimals)

                df = pd.concat([df, df_ticker[df_columns]], ignore_index=True)
                logging.info(f'''{chain}: {ticker.upper()}: observation count = {len(df_ticker)}, price range: {df_ticker['price_decimal'].min()}-{df_ticker['price_decimal'].max()}''')

                # if ticker == 'eth-usd':
                #     df_eth_usd = df_ticker
                # elif base == 'usd': # usd2eth cross rate
                #     ticker = ticker.replace('-usd','-eth')
                #     if ticker not in chain_contracts.keys():
                #         df_cross = pd.DataFrame(columns = df_columns)
                #         for k, t in df_ticker[df_columns].iterrows():
                #             e = df_eth_usd[df_eth_usd['updated_at'] <= t['updated_at']]
                #             if len(e) > 0:
                #                 i = (t['updated_at'] - e['updated_at']).idxmin()
                #                 e_price_decimal, e_decimals = e.loc[i][['price_decimal','decimals']]
                #                 t.ticker = ticker
                #                 t.price_decimal = t.price_decimal/e_price_decimal
                #                 t.decimals = 18
                #                 t.base = 'eth'
                #                 t.type = 'cross'
                #                 t.price = int(t.price_decimal*10**(t.decimals))
                #                 df_cross = pd.concat([df_cross, t])
                #         df = pd.concat([df, df_cross], ignore_index=True)
                #         logging.info(f'''cross rate {ticker.upper()}: observation count = {len(df_cross)}, price range: {df_cross['price_decimal'].min()}-{df_cross['price_decimal'].max()}''')
                # elif base == 'eth': #eth2usd cross rate
                #     ticker = ticker.replace('-eth','-usd')
                #     if ticker not in chain_contracts.keys():
                #         df_cross = pd.DataFrame(columns = df_columns)
                #         for k, t in df_ticker[df_columns].iterrows():
                #             e = df_eth_usd[df_eth_usd['updated_at'] <= t['updated_at']]
                #             if len(e) > 0:
                #                 i = (t['updated_at'] - e['updated_at']).idxmin()
                #                 e_price_decimal, e_decimals = e.loc[i][['price_decimal','decimals']]
                #                 t.ticker = ticker
                #                 t.decimals = e_decimals
                #                 t.base = 'usd'
                #                 t.type = 'cross'
                #                 t.price_decimal = t.price_decimal*e_price_decimal
                #                 t.price = int(t.price_decimal*10**(t.decimals))
                #                 df_cross = pd.concat([df_cross,t])
                #         df = pd.concat([df, df_cross], ignore_index=True)
                #         logging.info(f'''cross rate {ticker.upper()}: observation count = {len(df_cross)}, price range: {df_cross['price_decimal'].min()}-{df_cross['price_decimal'].max()}''')

    df = df.sort_values(by = 'updated_at').reset_index(drop=True)
    pd.set_option("display.precision", 18)

    logging.info(df)

    credentials = service_account.Credentials.from_service_account_file(
        'gearbox-336415-5ed144668529.json',
    )
    gcp_project_id = 'gearbox-336415'
    if len(df)>0:
        numeric_cols = ['price', 'price_decimal','heartbeat','decimals','blockNumber']
        df[numeric_cols] = df[numeric_cols].astype('float64')

        pandas_gbq.to_gbq(df,
                          'gearbox.oracle_price_history',
                          project_id=gcp_project_id,
                          if_exists = ('replace' if from_block==0 else 'append'),
                          progress_bar = False)
        logging.info('gearbox.oracle_price_history, insert done')
    else:
        logging.info('No new events since block', from_block)

if __name__ == '__main__':
    logging.basicConfig(
        format='%(asctime)s %(funcName)s %(levelname)s %(message)s',
        level=os.environ.get('LOGLEVEL', 'INFO').upper(),
        datefmt='%Y-%m-%d %H:%M:a%S',
        )
    w3_connection = {'ethereum' : Web3(Web3.HTTPProvider(RPC_Endpoint, request_kwargs={'timeout': 30}))}
    logging.info (f'Ethereum connected: {w3_connection["ethereum"].is_connected()}')

    main()
