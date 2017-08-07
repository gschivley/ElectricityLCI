# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 22:50:07 2017

@author: Tapajyoti
"""

import pandas as pd
import numpy as np



df1 = pd.read_csv("StandardDev_LCI1.csv",error_bad_lines=False)
df2 = pd.read_csv("StandardDev_LCI2.csv",error_bad_lines=False)
df3 = pd.read_csv("StandardDev_LCI3.csv",error_bad_lines=False)
df4 = pd.read_csv("StandardDev_LCI4.csv",error_bad_lines=False)


df1 = pd.melt(df1, id_vars=[' eGRID subregion acronym ',' Plant primary fuel '], 
            value_vars=list(df1.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df2 = pd.melt(df2, id_vars=[' eGRID subregion acronym ',' Plant primary coal/oil/gas/ other fossil fuel category '], 
            value_vars=list(df2.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df3 = pd.melt(df3, id_vars=[' eGRID subregion acronym ','Solar type'], 
            value_vars=list(df3.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df4 = pd.melt(df4, id_vars=[' eGRID subregion acronym ','Geo type'], 
            value_vars=list(df4.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')



df1.to_csv('FinalLCIsdforimport1.csv')
df2.to_csv('FinalLCIsdforimport2.csv')
df3.to_csv('FinalLCIsdforimport3.csv')
df4.to_csv('FinalLCIsdforimport4.csv')


'''

import pandas as pd
import numpy as np



df1 = pd.read_csv("StandardDev_NEI1.csv",error_bad_lines=False)
df2 = pd.read_csv("StandardDev_NEI2.csv",error_bad_lines=False)
df3 = pd.read_csv("StandardDev_NEI3.csv",error_bad_lines=False)
df4 = pd.read_csv("StandardDev_NEI4.csv",error_bad_lines=False)


df1 = pd.melt(df1, id_vars=[' eGRID subregion acronym ',' Plant primary fuel '], 
            value_vars=list(df1.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df2 = pd.melt(df2, id_vars=[' eGRID subregion acronym ',' Plant primary coal/oil/gas/ other fossil fuel category '], 
            value_vars=list(df2.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df3 = pd.melt(df3, id_vars=[' eGRID subregion acronym ','Solar Type'], 
            value_vars=list(df3.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')
df4 = pd.melt(df4, id_vars=[' eGRID subregion acronym ','Geo type'], 
            value_vars=list(df4.columns[3:]), 
            var_name='Emissions', 
            value_name='Emissions quantity kg/MWh')



df1.to_csv('FinalNEIsdforimport1.csv')
df2.to_csv('FinalNEIsdforimport2.csv')
df3.to_csv('FinalNEIsdforimport3.csv')
df4.to_csv('FinalNEIsdforimport4.csv')

'''