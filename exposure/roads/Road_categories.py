import numpy as np
import geopandas as gp
import pandas as pd


gridList=['5x5_grids_01']



rdc=pd.read_csv('road_dict.csv')
bus=rdc.set_index('type')['cat2'].to_dict()
bus['nan'] = 'unclassified'



for city in gridList:
    outSHP=f'{grid}_roadDensity.shp'
    rd2=gp.read_file(outSHP)
    stli=rd2.fclass.tolist()
    stli2=[]
    for lt in stli:
        L2 = str(lt)
        L3=[L2]
        stli2.append(L3)
    a=[]
    for lt in stli2:
        L2 = [bus[x] for x in lt]
        a.append(L2)
    ab=np.array(a)
    rd2['RoadType']=ab
    roadcats=(rd2.drop_duplicates('RoadType'))['RoadType'].tolist()
    for roadcat in roadcats:
        rd3=rd2[rd2['RoadType']==roadcat]
        rd4=gp.GeoDataFrame(rd3)
        rd4.to_file(driver = 'ESRI Shapefile', filename=f'{grid}_roadcat.shp')
