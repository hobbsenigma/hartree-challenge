import pandas as pd
import numpy as np
from itertools import chain, combinations

dataset1 = pd.read_csv('dataset1.csv')
dataset2 = pd.read_csv('dataset2.csv')

merged_dataset = pd.merge(dataset1, dataset2, how='inner', on='counter_party')

merged_dataset['value_ARAP'] = np.where(merged_dataset['status'] == 'ARAP', merged_dataset['value'], 0)
merged_dataset['value_ACCR'] = np.where(merged_dataset['status'] == 'ACCR', merged_dataset['value'], 0)

# convert column to string data type to avoid having it cast as a nullable float later
merged_dataset['tier'] = merged_dataset['tier'].astype('str')

# given a data frame, a set of columns to subtotal by, and an aggregate dictionary suitable for DataFrame.agg(),
# return a data frame with totals for every subset of columns
def get_totals(data: pd.DataFrame, subtotal_columns: list, aggregate_dict: dict) -> pd.DataFrame:
        # return all non-empty combinations of columns, e.g. [A, B, C] -> [(A,), (B,), (C,), (A, B), (A, C), (B, C), (A, B, C)]
        def get_column_subsets(columns: list) -> list[tuple]:
                return list(chain.from_iterable(combinations(columns, i) for i in range(1, len(columns)+1)))
        
        # add a dummy column in order to return aggregates for the entire data set
        # this allows us to use the same groupby logic as for subtotals
        DUMMY_COLUMN_NAME = '__DUMMY__'
        subtotal_combinations = [(DUMMY_COLUMN_NAME,)]
        data[DUMMY_COLUMN_NAME] = 0

        subtotal_combinations += get_column_subsets(subtotal_columns)

        container = []
        # start with the longest column combination to keep the columns in the original order
        for combo in reversed(subtotal_combinations):
                subtotal = data \
                        .groupby(list(combo)) \
                        .agg(aggregate_dict) \
                        .reset_index()
                container.append(subtotal)

        # combine all results, drop dummy column, and replace NaN with 'Total'
        return pd.concat(container).drop(DUMMY_COLUMN_NAME, axis=1).fillna('Total')

totals = get_totals(merged_dataset, 
                       ['legal_entity', 'counter_party', 'tier'], 
                       {'rating': 'max', 'value_ARAP': 'sum', 'value_ACCR': 'sum'}
                      )

totals.rename(columns={"rating": "rating_MAX"}).to_csv('output.csv', index=False)



