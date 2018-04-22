#!/usr/bin/python2

#~ an extension to agate which provides the following table functions:
#~ update_where : emulates sql update ... where ...


import agate

def update_where_function(row):
    if (row['test_col'] == row['test_val']):
        return row['update_val']
    else :
        return row['update_col']
        

def update_where(self,update_col,update_val,test_col,test_val):
    # check the types of the update and test columns.
    colnames = self.column_names
    types = [self.column_types[colnames.index(update_col)],self.column_types[colnames.index(test_col)]]
    # start the magic
    self = self.join(self.select([update_col, test_col]) \
        .rename(column_names = {update_col: 'update_col', test_col: 'test_col'})
        )
    self = self.join(agate.Table([[update_val, test_val]]*len(self.rows),['update_val','test_val'], types))
    self = self.compute([
        ('updated', agate.Formula(agate.Number(), update_where_function))
    ])
    self = self.rename(column_names = {update_col: 'old', 'updated': update_col}) \
        .exclude(['old', 'update_col', 'test_col', 'update_val', 'test_val'])
    return self
    
agate.Table.update_where = update_where
