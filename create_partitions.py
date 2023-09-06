import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import s3fs
import time

users_100 = {b'\xf9\xaa\xac\xb7D_I\x82\xb8G;\xbc\x01\x15\xaa\x8b', b'\xac\xe0*\xcb\xa9\xe0J\xf4\xa3\xfe\xdf\x08\xf0\xa0\xaeq', b"`kDJW\x15@B\xa3\xfbv\xe5'\x8e\x90\xe6", b'\xd3t\xa6v\x17mO\xc0\xb2\x99\xb6\xb1\x95Z|\xbd', b'n\xe2t9\x9fsC8\x8c~\x07\xd8Y\xe4\xccC', b'v"\xf0\x1d\xc0\xa8AT\xab\xf7\xaa\x0c\r\r\n\xc2', b'QN\xa1\xbdWIF\xc1\xb3zR%\xac\xd95\xe4', b'=^\xc5\xd2\xc9\xebE\x9f\xaeY\x13\xe3\xf1q/\xc1', b'\x90\xb9\xa3;a\xbfDS\xa3\xcd\xbd\x02c?]1', b'.\x15kX&1G\xc9\xab\x1c\x80Gn]\x95\xd7', b'\x16\xae\xb6*\xd0\xfbN\xac\x94\xd1\x85\n\xa9\x84\xca\xf1', b'\x8a\x95\xaa\x8bkYO\xdb\x97\xce\xff9N\x82|\x12', b'r\xc2\x16\xfc7:D\xfe\x87\xf9Xx\xba\xe7$K', b'c\xed\xf2\xdcG\x84F:\xbaz\xb1\xcaJ\xfbzw', b'4\xcf\x16q\x1d\xeaG=\xa1\xf9\x8f>E\xa2\xb4j', b'v\xf4\x92\xfc\x13\x8fD\x0e\xbf\x98\x1c\x13"\x87@\xa9', b'O2\xefp\xdb\xe3M\xb7\xb11U\xc07\xc7\x88\x1a', b"\xda'\x10\x90\x13F@\xe9\xb8\x94\x94]\xd8\x9dY\xa6", b'\x9f\xbe\x01\x89c\xb2J\xf2\x84K\x01\x1cGh\xed\xce', b'\xb0\xfbU\xbe\xc9}J\x0e\x9fq*?\x0b\xbc\x85\r', b'\xf2\xad\x93H\xe2\xadN\xe2\xa0\xc8\x18\x8c\x13\x18?~', b'S\xcfH\x95\xa1\xd9Jg\xa7V\xbeO\x0b\xfbHL', b'\xa4\x06\xf4\x7f`\xdbH{\xbf\xd3\xe2\xf9\x9bR\xda\xe8', b'\xc9\xed!X\x9a\x8bK\xe7\x89Z\x95\xd1\xe9\xf2\xc7\x08', b'U\xe7\x12\xc35\xe7I]\xa2.\xfb\x98\xdf\x93\x83\xba', b'F\xc8,X\xff\tE8\xa9\xd7\xfe^\x1eq\xb8\x17', b'\x8b\xa9o\xb9\xe2\xd3Kr\x8b\x02y\x99\x96\xd2ne', b'e\x1a\xf84\x80\xa1G\x0f\x82\x16+\x1d\xe9\x1biG', b'\x1b\xfd\xb3\xe7$\xafM\x11\x8f\x99\xcd\t\x81\xb9\xa5a', b'\xd3>AN\x81kE\xa5\x89\n\xf9\x05-\xe4J\xb1', b'\x92&+\xd0\x9f\xfeF\xf8\x8b\x7f\xea\xf88\x80\xaa\x1f', b'\xe2e\xc7\xa6\xaf\xe3FI\x84\xa2\x08\x14\x0f\x9fh\x18', b'\xf6c\xcaf\x85\x14Ht\x88\x19\xe4(\xee\xf0\x91$', b'M{\x87+\x91\xf2N\xd3\x95\x94\xc9\xfc\x04\xec>I', b'gT\xc9\x98\x17%B]\xa7+\x9e\xf6\xe9\xd5\xecm', b'\x080\xd7r\x1f\xb7B\x87\xb5\xd3P\xbbz\x90\x9e\xaa', b'r\x83\xda\x7fn\xadC\xde\x9dL@a\xecX\x18\x19', b'\xe6\xb2\x01\xa2\x05iB\x85\xac\reV\x1a#\x19&', b'y~\xc3lsmJ\xe2\x9f\x06"\xe5n\xba\xd5\x9c', b'io\xb3\xc4#\x93H\x0b\x8e\x89 \xe7#\x8f\x14\x1a', b'\x04P\xf0\x81z\x05E\x87\xab\xc1\x82t\xd9\xee\xbdy', b'\x9c\xc6\x1b\\]\xe4H\xf0\xbb\x84\xf9\xf6/@j\x81', b'\xb9\xb8E\x94o\xc1AP\xbf\x0fg\r\xf7\x96\xdb\x9b', b'\xe6\xcbX\x9b|,O\xaa\xaa\x93f\xa1\xe1\x9b\xf6+', b'\xb5\xb1\x90-g\xb4J\xa9\xa3\x1b\x9e\xa97VB\x81', b'\xe4\xd9\x0f\xa9R\xbe@\xaf\xa3\xcf>?%\x89\\\xb1', b'\xe0\x0c\x15aIEH\x14\x81t\xb7F\x0f\x97\x89\xd4', b'\xec\r<)\xb73LZ\xb7\xf2\xe2\xdc\x15\xfdqC', b'\x99}\x9bs\xcbzE\x9a\xb8\xe9\x86\xbco\x96$\x86', b'\xf1.\xbcn,PE\xb6\xad6\x98\x9b\x8eB;\x0b', b'!P\x04\xc1\xf1\x02Mn\x8d;\xcd\xb2\xae\x83\x8f\xa2', b'@\xe9\xda\x9e\x9d\xe8D\xd3\x81sx\xe4p\xb8\xf9<', b'P\x10\xcb\xdfT\xdfA\x13\xbe\xeb\x83\r\x13b\xf8\x8b', b'\xbe\xe5>g\xfc\x9bI^\xb6y\x81MM@\x1f\xb7', b'\x1fZ\x8aCb\xc7L\x0c\xba\xd1j@\xe8\x11\x0c2', b'e\xd9~3\xf4iGW\x8e\x9f\xd6\xa56\\\x0c\xff', b'\x17~\xbe\xe9\xd7\xb9E\x16\xb5a\x8d\xa4\x9e\x9a\x18$', b'YA\xa1\xf8\xc6^N\x8f\x8dr\xb5$-[ku', b'\xf1%2\xd0W0H\x9e\x8fh\xcaZ\xc2\x85\x9c\x8d', b'\xda\x83\xa0\x16%=Nc\xa6\x0b\x10\n\xf2\xc1\xcc\x89', b'4\xadP\x1b\x84R@\xb4\x8f\x9dh1\xcd\x91\xa5\xd0', b'\xe7\xeaE3NiI\xda\xb3U\xef\x08\xa0\xc9I\xf9', b'\x94\xc5J\xc04\xe9H\x1b\x9c\x9aV-\x0c\xa7I\xdb', b'\xdb\xc2\x02\xa8.\xbbK\xd9\x98\xac\xae\x0f\xa0\xc2V"', b'_j\xfb\x91\x8e\xcfDV\x97Qz\x92\x96=]\xaa', b'\x17\x02\\B\xa5\xe5L-\x8d\x95`\x00\xe8\xec]\xff', b's\xd3W\x15\xed\xd5G\xff\xaf4\x9a\x8d\xf5\x9f\x0c=', b'\xdb@`\x05\xc7\x1bC\xd0\xb3\xd9L\x8cw^\xa4\xbd', b'\xf1\x04\x84g\xd7\xb6G\xf2\x96\xc4Y\x9b\xd5\x94yD', b'\xc2?c\xbf\x8f\x85I\x9f\x9c\x95\x19$utvX', b'\xe2<\x84\xf14II\xf2\xb4\xdfJ\x8b\x06\xc8\x8c\xbe', b'\x1c\xcb\x13\xc2\xc0vGH\x8d\xa9\x07d/5\xf1u', b' \xfe\xa4\\}\x13M\xab\x9b\n\x9d\x1e^,\xdf\xb4', b'\x97=\xb8_\xe9nJ\xde\x9cr\x0e\r}\x19\xc1\xc5', b'a\xde\x11\xc3\xee I\xde\xa0\xd0\xe8a\xc7\xf7\xf0\x97', b'\xff\x02E\x8b\\s@\x10\x91gm\xa1\x9f`\xadq', b'\x8f\x97\xfd\x1d\x89+C\x8b\x82\xca\x1e\xc0\x97\x16\x07\x81', b'N\xe7H\xeb+}@\xce\x9e\xc4\xf1nVP\xd6\xa2', b'\x940@\xbbM1B\xe8\x86\xa4\xd3^\xee2\x0c\x00', b'e/\xc0\xeeE4Js\xa0M\xd7\xdb\x97p\x9f%', b'\xa6\n\x9f\xa3\xa8NH\xcb\x8c\xde\xa7\xcb\xf9A\xce\xb8', b'\xcb\xfaa\x9b\xdb\x15AS\x96\xa4\x95\x17|\xbb\xe5}', b"6S\xd8'pXH\xec\x89F\xe2N\x14.\xff\xee", b's\xfd?\x81\x8e\xe9K\x1e\xb77\xea\xe8\xc3&*\xcb', b'\xca\xb0q\xfa\xfc#E\xdf\x82\x84i0&\xcd\x12\xe5', b'\xc6\xcc\xc9\xf0\x1cBG\x94\xab\x14\xad\xc11\xca\xed\x19', b'\x02\x8e\xc4\xd4\x94\x08N\xb1\x86\xb4e\xb6TK\xb0\x8b', b'y\xb7\x9br\x97\x88J\x9c\x94+\t\xa9\x80\xcf\xdcu', b'\xad\x02\x08\x1c4\xfaA\xde\xb7\xd4[a\xfb\xfbN_', b'\x03\xa1e`d\xb7D:\xbc\xf8\xc4n\xd42\x84\x18', b'h\nC\x1e\xde\xf9O:\x88\xe4\x90\x8d\xc50\xf7\xc1', b'\xc9v\xef5PoO\xcf\xa7Hd\xc2L!zG', b'\xea\x8f\xf2LPQI\xf8\x87\t\xf6\xe3E\xa3\xc6P', b'\xbf\xaf_j\x1b\xc0KF\xa5"n\xcf\xbd\x12\xa3Q', b'\x18k\x80\xca\x153BF\x92\xd1\xd0N\x04J\xe4?', b'\x882\xee\xff\x83\xa2O\xe0\x9d)\xca\x0e\x10\x80\x91\xc7', b'\x053w\xdb\xa1lD\xbb\x85\x8c\xd7/\x94\xb2\xf7e', b"\xe2(\x83\x01\x9e\x1cO\xa4\x84'\x11Y\x99+\x16\xb7", b'\xcbm\x95#G M[\xba\xa3/#\x84!r\xcf', b'2\x1c\xa5\xe7:mLd\xab\xa65m\xc2\xc8\x0cL'}

user_filter = ds.field('user_uuid').isin(users_100)

s3 = s3fs.S3FileSystem()

bucket = 'quasar-sandbox-events'
path_ = 'v2021-01/parquet/started_session'
path_2022 = 'v2021-01/parquet/started_session/year=2022'
path_2022_11 = 'v2021-01/parquet/started_session/year=2022/month=11'
path_2022_11_28 = f'{path_2022_11}/day=28'

import threading

pa.set_io_thread_count(128)
start=time.time()
def repartition_day(year,month,day):
    path = f'{path_}/year={str(year)}/month={str(month).zfill(2)}/day={str(day).zfill(2)}'
    print(f'Starting {path}')
    dataset = pq.ParquetDataset(f'{bucket}/{path}', filesystem=s3)
    table = dataset.read_pandas(use_threads=True)
    print(f"Read {len(table)} events from {path}")
    part_path=f's3://started_session_2022_11_uuid/year={str(year)}/month={str(month).zfill(2)}/day={str(day).zfill(2)}'
    pq.write_to_dataset(table, root_path=part_path,partition_cols=['user_uuid'], use_threads=True)
    print(f"Wrote to {part_path}")


year_month_days  = ( list(zip([2022]*31, [10]*31, range(14,32))) + 
                     list(zip([2022]*31, [11]*30, (list(range(1,28))+[30]) )) 
                     )

threads = []

for year, month,day in year_month_days:
    thread = threading.Thread(target=read_day, args=(year, month, day))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()

end=time.time()
print(f"Elapsed time: {end-start} sec")
