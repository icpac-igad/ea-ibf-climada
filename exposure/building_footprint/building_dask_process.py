import glob
import geopandas as gp
import pandas as pd
import ntpath
import gc
import dask_geopandas

import dask.dataframe as dd

gzfiles=glob.glob('/home/data_folder/points_s2_level_4_gzip/*.csv.gz')

grid_db=gp.read_file('/home/data_folder/osm_data/ea_5x5_grid.shp')

for gzfl in gzfiles:
    print(gzfl)
    #bdb=pd.read_csv(gzfl)
    do_dask_compute0 = dd.read_csv(gzfl, compression='gzip')
    bdb=do_dask_compute0.compute()
    outputname=ntpath.basename(gzfl).split('.')[0]
    for idx, row in grid_db.iterrows():
        min_long,min_lat,max_long,max_lat=row['geometry'].bounds
        bdb1=bdb.loc[(bdb['longitude'] >=min_long ) & (bdb['longitude'] <= max_long)]
        bdb2=bdb1.loc[(bdb1['latitude'] >=min_lat ) & (bdb1['latitude'] <= max_lat)]
        if not bdb2.empty:
            print(bdb2.info())
            bdb2['dem_name']=row['dem_name']
            dem_name=row['dem_name']
            bdb2.to_csv(f'/home/data_folder/points_s2_level_4_gzip/ea_grid_filter_t2/{dem_name}_{outputname}.csv.gz', compression='gzip',index=False)
            del [[bdb1,bdb2]]
    gc.collect()
    del [[bdb]]
    bdb=pd.DataFrame()
