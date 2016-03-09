import json


def count_hits(input_file='related-ads-cdr.txt'):
    groups = 0
    ads = 0
    count = 0
    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        for line in i:
            groups += 1
            json_data = json.loads(line)

            ads += len(json_data['ad_images'])

            for ad in json_data['ad_images']:
                if ad['unique_terms'] != 0:
                    count += 1

    print "Term matches in " + str(count) + " of " + str(ads) + " ads total from " + str(groups) + " sellers."


def histogram(input_file='related-ads-cdr.txt'):

    terms = {}

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        for line in i:
            json_data = json.loads(line)

            for term, count in json_data['terms'].iteritems():
                if term in terms:
                    terms[term] += 1
                else:
                    terms[term] = 1

    max = 0
    for count in terms.values():
        if count >= max:
            max = count

    scale = 100.0 / float(max)

    print "Number of sellers using the term:"

    for term in sorted(terms, key=terms.get, reverse=False):
        print int(terms[term] * scale) * '=', terms[term], term


def get_hits_for_terms(terms, input_file='related-ads-cdr.txt'):

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        for line in i:
            json_data = json.loads(line)

            for term, count in json_data['terms'].iteritems():
                if term in terms:
                    print json.dumps(json_data, indent=4)
                    break

count_hits()

# Generate a chart showing prevalence of ATF keywords and phrases as used by sellers
histogram()

# Return the sellers that use these specific terms...
#get_hits_for_terms(['no questions asked', 'no background', 'bazooka'])

