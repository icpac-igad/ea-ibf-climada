import numpy as np
import geopandas as gp
import pandas as pd



qq=gp.read_file(gridshapefilepath)
rd0=gp.read_file(roaddensityshapefilepath)
rd0.crs = {'init': 'epsg:4326', 'no_defs': True}
rd= rd0.to_crs({'init': 'epsg:32644'})
rd['length1']=rd['geometry'].length/1000
rd1=rd[['gno','length1']]
rd1.columns=['Maille','length1']
#rd1.info()
rd2=pd.merge(qq,rd1,on='Maille',how='left')

rd3=rd2[[u'Maille_X', u'Maille_Y', u'length1','Maille']]

rd3.fillna(0, inplace=True)
rd4=rd3[[u'Maille_X', u'Maille_Y','Maille',u'length1']]

rd4.columns=[u'Maille_X', u'Maille_Y','Maille',u'length_km']

ed=rd4.groupby(['Maille','Maille_X','Maille_Y'])['length_km'].agg('sum')
ed1=ed.reset_index()
ed1.to_csv(csvoutputfilepath, index=False)

