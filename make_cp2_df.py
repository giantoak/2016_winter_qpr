from itertools import chain
import html
import ujson as json
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.linear_model import LinearRegression

# Load in the price data from armslist
df = pd.read_table('uncharted_data/armslist-prices.txt')
df.drop('category_1', axis=1, inplace=True)
df.manufacturer = df.manufacturer.fillna('?').apply(
    lambda x: ' '.join(html.unescape(x).strip().split()))

# Kill various bad / unnecessary things
odd_category_2s = {'Events', 'Farming Equipment', 'Fishing Gear', 'Gun Safes',  'Hunting Gear',
                   'Knives', 'Optics', 'Reloading', 'Tactical Gear', 'Targets and Range Equipment', 'Vehicles'}
df = df.ix[(df.category_2.apply(lambda x: x not in odd_category_2s)), :]
for col in ['category_2', 'category_3']:
    df.ix[:, col] = df.ix[:, col].fillna('')
df.caliber = df.caliber.fillna('?')
# df = df.ix[(df.category_2 != '') | (df.category_3 != ''), :]

df.ix[(df.price_usd < 0), 'price_usd'] = np.nan

manufacturer_mapping = {'Action:': '?',
                        'Caliber:': '?',
                        'MasterPiece Arms': 'Masterpiece Arms'}
df.manufacturer = df.manufacturer.apply(lambda x:
                                        manufacturer_mapping[x]
                                        if x in manufacturer_mapping
                                        else x)


# Split out the salient parts of the caliber measure
def split_caliber(caliber_str):
    parts = caliber_str.strip().split()
    nums = []
    no_nums = []
    for part in parts:
        if any(p.isdigit() for p in part):
            nums.append(part)
        else:
            no_nums.append(part)
    return [' '.join(nums), ' '.join(no_nums)]

df.caliber = df.caliber.apply(
    lambda x: x.lower().replace('wincester', 'winchester'))

df['caliber_nums'], df['caliber_chars'] = zip(*df.caliber.apply(split_caliber))

# Get group info for ads, and merge it in
jsns = [json.loads(x) for x in open(
    'uncharted_data/related-ad-groups.txt', 'r').readlines()]
reverse_cluster_dict = {}
for jsn in jsns:
    for x in jsn['sources'] + jsn['records']:
        reverse_cluster_dict[x['cdr_id']] = (
            jsn['group'], len(jsn['sources'] + jsn['records']))

df['cluster_id'], df['cluster_size'] = zip(
    *df.cdr_id.apply(lambda x: reverse_cluster_dict[x] if x in reverse_cluster_dict else (-1, -1)))


# Turn a bunch of categories into categories
for col in ['caliber_nums', 'caliber_chars', 'caliber', 'manufacturer', 'category_2', 'category_3', 'vendor_type', 'cluster_id']:
    df[col] = df[col].astype('category')


# Get the train / test data.
# Rework risk factors.
cp2_df = pd.read_excel('cp2_annotations/Seller Cluster Training Data Set.xlsx')
cp2_df.columns = ['cluster_id', 'risk_profile', 'risk_factors', 'cdr_ids']

risk_mapping = {
    'reseller,reseller-never-fired': 'reseller reseller never-fired',
    '(scam)': 'scam',
    'reseller-never-fired': 'reseller never-fired',
    'selling parts': 'selling-parts',
    'numerous rounds of ammo': 'numerous-rounds-of-ammo',
    'many ads for ammo': 'many-ads-for-ammo',
    'selling at a discount': 'selling-at-a-discount',
    'black powder': 'black-powder',
    'quick sale': 'quick-sale',
    'muzzle loader': 'muzzle-loader',
    'will trade for suppressor': 'will-trade-for-suppressor',
    'grenade launcher': 'grenade-launcher',
    'bb gun': 'bb-gun',
    '80% lowers': '80%-completed',
    '80% completed': '80%-completed',
    'possible nfa': 'nfa',
    'no serial #': 'no-serial',
    'cash or silver': 'cash-or-silver',
    'numerous gun ads': 'numerous-gun-ads',
    'mentioned minor adj to full auto': 'mentioned-minor-adj-to-full-auto',
    'no sn': 'no-serial',
    'no serial number': 'no-serial',
    'spam?': 'spam',
    'sbr?': 'short-barreled-rifle',
    'sig brace': 'sig-brace',
    'call number for guns': 'call-number-for-guns',
    'one ad has different location': 'one-ad-has-different-location'}

risk_mapping_order = sorted(list(risk_mapping.keys()), key=len)[::-1]


def fix_mapping(x):
    risk_str = ' '.join(x.lower().strip().split())
    for key in risk_mapping_order:
        if risk_str.find(key) > -1:
            risk_str = risk_str.replace(key, risk_mapping[key])
    return list(set(risk_str.split()))

cp2_df.risk_factors = cp2_df.risk_factors.apply(fix_mapping)

all_risk_categories = {'37mm',
                       '80%-completed',
                       'ammo',
                       'bb-gun',
                       'black-powder',
                       'call-number-for-guns',
                       'cash-or-silver',
                       'ffl',
                       'grenade-launcher',
                       'launcher',
                       'many-ads-for-ammo',
                       'mentioned-minor-adj-to-full-auto',
                       'muzzle-loader',
                       'never-fired',
                       'nfa',
                       'no-serial',
                       'numerous-gun-ads',
                       'numerous-rounds-of-ammo',
                       'old',
                       'one-ad-has-different-location',
                       'overpriced',
                       'parts',
                       'pricing-aberration',
                       'quick-sale',
                       'reseller',
                       'scam',
                       'selling-at-a-discount',
                       'selling-parts',
                       'sells-across-state-lines',
                       'short-barreled-rifle',
                       'sig-brace',
                       'spam',
                       'will-trade-for-suppressor',
                       'xm855'}

for factor in all_risk_categories:
    cp2_df['factor_{}'.format(factor).replace(
        '-', '_')] = cp2_df.risk_factors.apply(lambda x: factor in x)

# Kill wildcard duplicate row
cp2_df = cp2_df.drop(394)

# count of all risk factors. most things just seen once.
pd.Series(list(chain(*cp2_df.risk_factors.tolist()))).value_counts()

# meta-list
# 67     1
# 15     3
#  7     1
#  6     1
#  3     1
#  2     8
#  1    19
pd.Series(list(chain(*cp2_df.risk_factors.tolist()))
          ).value_counts().value_counts().sort_index(ascending=False)

# Simplified risk profile, because we have so little data.
cp2_df['simple_risk_profile'] = cp2_df.risk_profile.apply(
    lambda x: 'low' if x == 'low' else 'medium_or_high').astype('category')

# Simple pivot table for graphs
simple_risk_pivot = pd.pivot_table(cp2_df, index='simple_risk_profile', values=[
                                   x for x in cp2_df.columns if x.find('factor_') > -1], aggfunc=sum, fill_value=0).T

# Simple heat map of risk correlations
sns.heatmap(simple_risk_pivot, cmap='bone_r')

# Normalized heat map
sns.heatmap(simple_risk_pivot / simple_risk_pivot.sum(), cmap='bone_r')

# MERGE THE DFS
new_df = pd.merge(df, cp2_df)


# OK. WRITE PICKLE
new_df.to_pickle('cp2_data.pkl')
