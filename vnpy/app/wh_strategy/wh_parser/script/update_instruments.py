import pickle

instruments = []
with open('./data/bundle/instruments.pk', 'rb') as f:
    instruments = pickle.load(f)

instrument_store = {i["order_book_id"]: i for i in instruments}

instrument_store.update({
    'AU99l': {'margin_rate': 0.04,
              'exchange': 'SHFE',
              'market_tplus': 0,
              'contract_multiplier': 1000,
              'maturity_date': '0000-00-00',
              'listed_date': '0000-00-00',
              'underlying_symbol': 'AU',
              'type': 'Future',
              'trading_hours': '21:01-02:30,09:01-10:15,10:31-11:30,13:31-15:00',
              'underlying_order_book_id': 'null',
              'order_book_id': 'AU99l',
              'symbol': '黄金指数连续log',
              'de_listed_date': '0000-00-00',
              'round_lot': 1
              },
    'CU99l': {'margin_rate': 0.05,
              'exchange': 'SHFE',
              'market_tplus': 0,
              'contract_multiplier': 5,
              'maturity_date': '0000-00-00',
              'listed_date': '0000-00-00',
              'underlying_symbol': 'CU',
              'type': 'Future',
              'trading_hours': '21:01-01:00,09:01-10:15,10:31-11:30,13:31-15:00',
              'underlying_order_book_id': 'null',
              'order_book_id': 'CU99l',
              'symbol': '铜指数连续log',
              'de_listed_date': '0000-00-00',
              'round_lot': 1},
    'C99l': {'margin_rate': 0.07,
             'exchange': 'DCE',
             'market_tplus': 0,
             'contract_multiplier': 10,
             'maturity_date': '0000-00-00',
             'listed_date': '0000-00-00',
             'underlying_symbol': 'C',
             'type': 'Future',
             'trading_hours': '09:01-10:15,10:31-11:30,13:31-15:00',
             'underlying_order_book_id': 'null',
             'order_book_id': 'C99l',
             'symbol': '玉米指数连续log',
             'de_listed_date': '0000-00-00',
             'round_lot': 1},
    'RB99l': {'margin_rate': 0.05,
              'exchange': 'SHFE',
              'market_tplus': 0,
              'contract_multiplier': 10,
              'maturity_date': '0000-00-00',
              'listed_date': '0000-00-00',
              'underlying_symbol': 'RB',
              'type': 'Future',
              'trading_hours': '21:01-23:00,09:01-10:15,10:31-11:30,13:31-15:00',
              'underlying_order_book_id': 'null',
              'order_book_id': 'RB99l',
              'symbol': '螺纹钢指数连续log',
              'de_listed_date': '0000-00-00',
              'round_lot': 1}
    ,

})

with open('./data/bundle/instruments.pk', 'wb') as f:
     pickle.dump(list(instrument_store.values()),f)