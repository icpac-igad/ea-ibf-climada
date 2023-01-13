import glob
import geopandas as gp
import pandas as pd
import ntpath
import gc
import dask_geopandas

import dask.dataframe as dd

gzfiles=glob.glob('/home/data_folder/points_s2_level_4_gzip/*.csv.gz')

grid_db=gp.read_file('/home/data_folder/osm_data/ea_5x5_grid.shp')

# for gzfl in gzfiles:
#     print(gzfl)
#     bdb=pd.read_csv(gzfl)
#     gbdb = gp.GeoDataFrame(bdb, geometry=gp.points_from_xy(bdb.longitude, bdb.latitude))
#     dgdf = dask_geopandas.from_geopandas(gbdb, npartitions=4)
#     db = dgdf.sjoin(grid_db, how="left")
#     db.drop(columns=['geometry', 'index_right','sno'], inplace=True)
#     db.fillna('out_of_ea', inplace=True)
#     dem_db=db.drop_duplicates('dem_name')
#     dem_name_list=dem_db['dem_name'].tolist()
#     dem_name_list=[i for i in dem_name_list if not i in 'out_of_ea']
#     outputname=ntpath.basename(gzfl).split('.')[0]
#     del [[bdb,gbdb,dem_db]]
#     gc.collect()
#     bdb=pd.DataFrame()
#     gbdb=pd.DataFrame()
#     dem_db=pd.DataFrame()
#     for dem_name in dem_name_list:
#         db1=db[db['dem_name']==dem_name]
#         db1.to_csv(f'/home/data_folder/points_s2_level_4_gzip/ea_grid_filter/{dem_name}_{outputname}.csv.gz', compression='gzip')
#         del [[db1]]
#         gc.collect()
#         db1=pd.DataFrame()


for gzfl in gzfiles[7:]:
    print(gzfl)
    #bdb=pd.read_csv(gzfl)
    do_dask_compute0 = dd.read_csv(gzfl, compression='gzip')
    bdb=do_dask_compute0.compute()
    print('completed dask read of csv.gz')
    gbdb = gp.GeoDataFrame(bdb, geometry=gp.points_from_xy(bdb.longitude, bdb.latitude))
    dgdf = dask_geopandas.from_geopandas(gbdb, npartitions=4)
    do_dask_compute = dgdf.sjoin(grid_db, how="inner")
    db = do_dask_compute.compute()
    print(db.info())
    if db.empty:
        pass
    else:
        db.drop(columns=['geometry', 'index_right','sno'], inplace=True)
        dem_db=db.drop_duplicates('dem_name')
        print(dem_db)
        dem_name_list=dem_db['dem_name'].tolist()
        outputname=ntpath.basename(gzfl).split('.')[0]
        del [[bdb,gbdb,dem_db]]
        gc.collect()
        bdb=pd.DataFrame()
        gbdb=pd.DataFrame()
        dem_db=pd.DataFrame()
        print('###########')
        for dem_name in dem_name_list:
            print(dem_name)
            print(db.info())
            db1=db[db['dem_name']==dem_name]
            db1.to_csv(f'/home/data_folder/points_s2_level_4_gzip/ea_grid_filter/{dem_name}_{outputname}.csv.gz', compression='gzip')
    del [[db1,db]]
    gc.collect()
    db1=pd.DataFrame()
    db=pd.DataFrame()
