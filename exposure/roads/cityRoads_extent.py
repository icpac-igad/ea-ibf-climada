import fiona
from shapely.geometry import shape, mapping
import rtree
import glob
import pandas as pd
import os
import ntpath


citylist=['5x5_grids_01.shp']


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)



def intersect(boundarySHP,outSHP,city):
    with fiona.open(boundarySHP, 'r') as layer1:
        # We copy schema and add the  new property for the new resulting shp
        schema = layer2.schema.copy()
        # We open a first empty shp to write new content from both others shp
        with fiona.open(outSHP, 'w', 'ESRI Shapefile', schema) as layer3:
            index = rtree.index.Index()
            for feat1 in layer1:
                fid = int(feat1['id'])
                geom1 = shape(feat1['geometry'])
                index.insert(fid, geom1.bounds)
            for feat2 in layer2:
                geom2 = shape(feat2['geometry'])
                for fid in list(index.intersection(geom2.bounds)):
                    if fid != int(feat2['id']):
                        feat1 = layer1[fid]
                        geom1 = shape(feat1['geometry'])
                        if geom1.intersects(geom2):
                            # We take attributes from ctSHP
                            props = feat2['properties']
                            # Then append the uid attribute we want from the other shp
                            props['code'] = city
                            geom3=geom1.intersection(geom2)
                            #props['length']=geom3.length*100
                            # Add the content to the right schema in the new shp
                            layer3.write({
                                'properties': props,
                                'geometry': mapping(geom1.intersection(geom2))
                            })

                                
roadSHP  = 'ea_gis_osm_roads_free_1.shp'

layer2= fiona.open(roadSHP, 'r')


for city in citylist:
    shpfiles1=(os.path.splitext(city)[0])
    shpfiles2=path_leaf(shpfiles1)
    boundarySHP=f'{shpfiles2}.shp'
    outSHP=f'{shpfiles2}_Road.shp'
    intersect(boundarySHP,outSHP,shpfiles2)
    print("completed "+shpfiles2)
    
