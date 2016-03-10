import pandas as pd
import seaborn as sns
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.cross_validation import StratifiedShuffleSplit
from sklearn.metrics import confusion_matrix

# I don't know why there are nans, but there are. Appalling
df = pd.read_pickle('cp2_data.pkl').dropna().reset_index(drop=True)


indices = df.ix[(df.simple_risk_profile == 'medium_or_high'), :].index.tolist()
npr.shuffle(indices)
indices_2 = df.ix[(df.simple_risk_profile == 'low'), :].index.tolist()
npr.shuffle(indices_2)
index_to_use = indices + indices_2[:len(indices)]

cols_to_use = ['price_usd', 'caliber_nums', 'caliber_chars', 'cluster_size',
               'vendor_type', 'manufacturer']

df.simple_risk_profile = df.simple_risk_profile.apply(
    lambda x: 0 if x == 'low' else 1)

df_for_test = df.ix[:, cols_to_use + ['simple_risk_profile']]
df_for_test = df_for_test.iloc[index_to_use, :]

sss = StratifiedShuffleSplit(
    df_for_test.ix[:, 'simple_risk_profile'], random_state=0)

dummy_df = pd.get_dummies(df_for_test.ix[:, cols_to_use])

c_m_list = []
etc = ExtraTreesClassifier(n_estimators=50)

for train_index, test_index in sss:
    etc.fit(dummy_df.iloc[train_index, :],
            df['simple_risk_profile'].iloc[train_index])
    y_preds = etc.predict(dummy_df.iloc[test_index, :])
    c_m_list.append(confusion_matrix(
        df_for_test['simple_risk_profile'].iloc[test_index], y_preds))


# test on scams
sss = StratifiedShuffleSplit(df.ix[:, 'factor_scam'], random_state=0)
