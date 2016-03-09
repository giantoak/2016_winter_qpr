from itertools import chain
import html
import ujson as json
import pandas as pd
import seaborn as sns

# Counts of ads / site over time (CDR pull results)

buckets = json.loads(open('cdr_query_results/weapons_per_site_per_month.json', 'r').read())[
    'aggregations']['per_site']['buckets']

for site in buckets:
    site['ads_over_time'] = site['ads_over_time']['buckets']

for site in buckets:
    for site_bucket in site['ads_over_time']:
        site_bucket['site'] = site['key']

all_buckets = list(chain(*[site['ads_over_time'] for site in buckets]))
df = pd.DataFrame.from_records(all_buckets)
del df['key']
df['month_year'] = df.key_as_string.apply(
    lambda x: pd.to_datetime(x.split('T')[0]))
df['month_year_str'] = df['month_year'].astype(str).apply(lambda x: x[:7])
sns.heatmap(df.pivot('site', 'month_year_str', 'doc_count').fillna(
    0).astype(int).sort_values(['2015-10', '2015-09'], ascending=False), annot=True, fmt='d', cmap='bone_r')

# merge weapon recovery graphs
# The heatmap is kind of boring - most weapons are recovered from the
# state in which they were sold.


def get_melted_weapons(fpath, year):
    df = pd.read_excel(fpath)
    df.index = [' '.join(x.strip().split()) for x in df.index.tolist()]
    df.columns = [' '.join(x.strip().split()) for x in df.columns]
    df.reset_index(inplace=True)
    df['recovery_state'] = df['index']
    del df['index']
    df = df.ix[df.recovery_state.apply(lambda x: x not in ['TOTAL', 'TOTALS']),
               [x for x in df.columns if x not in ['TOTAL', 'TOTALS']]
               ]
    melted_df = pd.melt(df, 'recovery_state')
    melted_df.columns = ['recovery_state', 'source_state', 'total']
    melted_df['year'] = year
    return melted_df

df = pd.concat(get_melted_weapons(
    'atf_data/{}_state_recoveries.xlsx'.format(year), year) for year in [2013, 2014])

mapping_dict = {'DST OF COLUMBIA': 'DISTRICT OF COLUMBIA',
                'GUAM': 'GUAM & NORTHERN MARIANA ISLANDS', 'US VIRGIN ISLND': 'US VIRGIN ISLANDS'}
for col in ['source_state', 'recovery_state']:
    df.ix[:, col] = df.ix[:, col].apply(lambda x: mapping_dict[x] if x in mapping_dict else x)

df.total = df.total.fillna(0).astype(int)

# floor total values at intersections
df.total = df.apply(lambda x: x['total'] if x['source_state'] != x['recovery_state'] else 0, axis=1)

sns.heatmap(pd.crosstab(df.recovery_state, df.source_state,
                        values=df.total, aggfunc=sum).fillna(0).astype(int),
            cmap='bone_r')

# Load the ATF's weapon manual, the list of stolen guns, and break out types

manual_df = pd.read_excel('atf_data/MANU_03-07-2016.xlsx')
manual_df.columns = ['manufacturer_code',
                     'manufacturer_name', 'country_of_origin']
code_to_manufacturer_dict = manual_df.ix[:, ['manufacturer_code', 'manufacturer_name']].set_index(
    'manufacturer_code').to_dict('dict')['manufacturer_name']


df_old = pd.read_excel('atf_data/2005-2016 Stolen FFL guns NOT Recovered.xlsx')
df_old.columns = ['serial_number', 'make',
                  'model', 'weapon_type', 'caliber_gauge']

mapping_dict = {'AW': 'Any Other Weapon', 'C': 'Combination Gun',
                'DD': 'Destructive Device', 'M': 'Machine gun', 'P': 'Pistol',
                'PD': 'Derringer', 'PR': 'Revolver', 'R': 'Rifle',
                'RF': 'Receiver/Frame', 'S': 'Shotgun', 'SI': 'Silencer',
                'TG': 'Tear Gas Launcher', 'Z': '?'}

df_old.weapon_type = df_old.weapon_type.apply(
    lambda x: mapping_dict[x] if x in mapping_dict else x)
df_old['manufacturer'] = df.make.apply(lambda x: code_to_manufacturer_dict[
                                       x] if x in code_to_manufacturer_dict else x)

# Value counts for weapon type seem like the best use of our time, and
# that's it...

# Loading up current weapons data,
# we can get value counts or diff it from the old data
# Still, there's not much else there.
# We can't really tie it to thefts.

df_new = pd.read_excel('atf_data/weapons_theft_data.xlsx')
df_new.columns = ['manufacturer', 'caliber_gauge ',
                  'model', 'weapon_type', 'firearm_count']

df_new.ix[:, ['weapon_type', 'firearm_count']].groupby('weapon_type').sum()


# Load in the price data from armslist

df = pd.read_table('uncharted_data/armslist-prices.txt')
df.drop('category_1', axis=1, inplace=True)
df.manufacturer = df.manufacturer.fillna('?').apply(
    lambda x: ' '.join(html.unescape(x).strip().split()))

# Kill various bad / unnecessary things
odd_category_2s = {'Antique Firearms', 'Events', 'Farming Equipment',
                   'Fishing Gear', 'Gun Safes',  'Hunting Gear', 'Knives',
                   'Optics', 'Reloading', 'Tactical Gear',
                   'Targets and Range Equipment', 'Vehicles'}
df = df.ix[(df.category_2.apply(lambda x: x not in odd_category_2s)), :]
for col in ['category_2', 'category_3']:
    df.ix[:, col] = df.ix[:, col].fillna('')
df.caliber = df.caliber.fillna('?')
df = df.ix[(df.category_2 != '') | (df.category_3 != ''), :]
df = df.ix[df.price_usd > 0, :]
df.vendor_type = df.vendor_type.astype('category')

manufacturer_mapping = {'Action:': '?',
                        'Caliber:': '?',
                        'MasterPiece Arms': 'Masterpiece Arms'}
df.manufacturer = df.manufacturer.apply(lambda x:
                                        manufacturer_mapping[x]
                                        if x in manufacturer_mapping
                                        else x)


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


jsns = [json.loads(x) for x in open(
    'uncharted_data/related-ad-groups.txt', 'r').readlines()]
reverse_cluster_dict = {}
for jsn in jsns:
    for x in jsn['sources'] + jsn['records']:
        reverse_cluster_dict[x['cdr_id']] = jsn['group']

df['group'], df['group_size'] =
zip(*df.cdr_id.apply(lambda x: reverse_cluster_dict[
    x], len(reverse_cluster_dict[x]) if x in reverse_cluster_dict else -1, -1))

for col in ['manufacturer', 'category_2', 'category_3', 'vendor_type']:
    df[col] = df[col].astype('category')



df = df.ix[(df.category_2.apply(lambda x: x not in odd_category_2s)), :]
df = df.ix[(df.category_2 != '') | (df.category_3 != ''), :]

sns.violinplot(x='manufacturer', y='price_usd',  data=df.ix[(df.manufacturer.apply(lambda x: x in manufacturers_to_use)), ['manufacturer', 'price_usd']].dropna())


##

nationalguntrader = json.loads(
    open('hg_scrapes/items_www.nationalguntrader.com_1.json', 'r').read())
shooterswap = json.loads(
    open('hg_scrapes/items_shooterswap.com_1.json', 'r').read())
shooting = json.loads(
    open('hg_scrapes/items_www.shooting.org_4.json', 'r').read())

##

lnsk_df = pd.DataFrame.from_records(json.loads(
    open('hg_scrapes/items_lionseek_3.json', 'r').read()))
lnsk_df.drop(['_type', 'image'], axis=1, inplace=True)

lnsk_df.ix[(lnsk_df.price.astype(int).fillna(-1) > 20000), :].head()


##
