import fiona
from shapely.geometry import shape, mapping
import rtree
import glob
import pandas as pd
import os
import ntpath


def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

cityList=['5x5_grids_01.shp]

def intersect(boundarySHP,roadSHP,outSHP):
    with fiona.open(boundarySHP, 'r') as layer1:
        with fiona.open(roadSHP, 'r') as layer2:
        # We copy schema and add the  new property for the new resulting shp
            schema = layer2.schema.copy()
            schema['properties']['gno'] = 'int:10'
            schema['properties']['length'] = 'int:10'
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
                                geom3=geom1.intersection(geom2)
                                if geom3.geom_type=='GeometryCollection':
                                    print("empty geometry")
                                elif geom3.geom_type=='Point':
                                    print(props)
                                else:
                                    props['gno'] = feat1['properties']['Maille']
                                    props['length']=geom3.length*100
                                    layer3.write({
                                    'properties': props,
                                    'geometry': mapping(geom1.intersection(geom2))
                                    })
                                

                                
for city in cityList:
    shpfiles1=(os.path.splitext(city)[0])
    shpfiles2=path_leaf(shpfiles1)
    boundarySHP=f'{shpfiles2}.shp'
    roadSHP=f'{shpfiles2}_Road.shp'
    outSHP=f'{shpfiles2}_roadDensity.shp'
    intersect(boundarySHP,roadSHP,outSHP)
    print(shpfiles2+' done')
