import vectorbtpro as vbt
import pickle as pkl
import os

download_path = '/mnt/h/Crypto_data/okx'

spot_path = os.path.join(download_path, 'spot')
swap_path = os.path.join(download_path, 'swap')

os.makedirs(spot_path, exist_ok=True)
os.makedirs(swap_path, exist_ok=True)

# def extract_info(info):
#     info_list = []
#     symbols = info['datasets']['symbols']
#     for s in symbols:
#         if s in ['SPOT', 'PERPETUAL']:
#             continue
#         if 'SWAP' in s['id']:
#             id = s['id'].replace('-SWAP', '')
#             s['id'] = id.replace('-', '/')            
#         else:
#             s['id'] = s['id'].replace('-', '/')
#         start_date = datetime.datetime.strptime(s['availableSince'], '%Y-%m-%dT%H:%M:%S.%fZ')
#         if start_date < datetime.datetime(2023, 1, 1):
#             s['availableSince'] = '2022-12-31'
#         else:
#             s['availableSince'] = (start_date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        
#         end_date = datetime.datetime.strptime(s['availableTo'], '%Y-%m-%dT%H:%M:%S.%fZ')
#         if end_date < datetime.datetime(2025, 2, 5):
#             s['availableTo'] = end_date.strftime('%Y-%m-%d')
#         else:
#             s['availableTo'] = '2025-02-06'
#         info_list.append(s)
#     return info_list    

# info_swap = extract_info(get_exchange_details(exchange="okex-swap"))
# info_spot = extract_info(get_exchange_details(exchange="okex"))

# info_swap = [s for s in info_swap if s['id'].endswith('USDT')]
# info_spot = [s for s in info_spot if s['id'].endswith('USDT')]

# spot_symbols = {s['id'] for s in info_spot}
# swap_symbols = {s['id'] for s in info_swap}

# common_symbols = spot_symbols & swap_symbols

# info_spot = [s for s in info_spot if s['id'] in common_symbols]
# info_swap = [s for s in info_swap if s['id'] in common_symbols]


# with open("info_spot.pkl", "wb") as f:
#     pkl.dump(info_spot, f)

# with open("info_swap.pkl", "wb") as f:
#     pkl.dump(info_swap, f)

info_spot = pkl.load(open("info_spot.pkl", "rb"))
info_swap = pkl.load(open("info_swap.pkl", "rb"))

print(f"Number of Spot Symbols: {len(info_spot)}")
print(f"Number of Swap Symbols: {len(info_swap)}")

for info in info_spot:
    

    id = info['id']
    start = info['availableSince']
    end = info['availableTo']
    try:   
        save_id = id.replace('/', '-')
        save_path = os.path.join(spot_path, f"{save_id}.parquet")
        
        if not os.path.exists(save_path):
            data = vbt.CCXTData.fetch_symbol(
                symbol=id,
                exchange="okx",
                start=start,
                timeframe="1m",
                retries=5,
                tz='UTC',
                show_progress=True,
            )


            df = data[0]
            df.to_parquet(save_path)
    except Exception as e:
        print(f"Error fetching {id}: {e}")

for info in info_swap:
    id = info['id']
    start = info['availableSince']
    end = info['availableTo']
    
    try:
        save_id = id.replace('/', '-')
        save_id = f"{save_id}-SWAP"
        save_path = os.path.join(swap_path, f"{save_id}.parquet")

        if not os.path.exists(save_path):
            data = vbt.CCXTData.fetch_symbol(
                symbol=f"{id}:USDT",
                exchange="okx",
                start=start,
                timeframe="1m",
                retries=5,
                tz='UTC',
                show_progress=True,
            )


            df = data[0]
            df.to_parquet(save_path)
    except Exception as e:
        print(f"Error fetching {id}: {e}")
