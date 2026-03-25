# seeds/iso3_mapping.py
# Mapping of Natural Earth ISO3 codes in states shapefile
# that do not exist in dim_country.
# Maps to the correct dim_country.iso3

iso3_mapping = {
    # Caribbean & North America
    "ABW": "ANT",  # Aruba → Netherlands (NL not in your table? or ANT for Netherlands Antilles)
    "AIA": "GBR",  # Anguilla → UK
    "ATG": "ATG",  # Antigua and Barbuda → itself
    "BLM": "FRA",  # Saint Barthelemy → France
    "BMU": "GBR",  # Bermuda → UK
    "BRB": "BRB",  # Barbados → itself
    "CUW": "NLD",  # Curaçao → Netherlands
    "CYM": "GBR",  # Cayman Islands → UK
    "DMA": "DMA",  # Dominica → itself
    "GRD": "GRD",  # Grenada → itself
    "KNA": "KNA",  # Saint Kitts and Nevis → itself
    "LCA": "LCA",  # Saint Lucia → itself
    "MSR": "GBR",  # Montserrat → UK
    "VCT": "VCT",  # Saint Vincent and the Grenadines → itself
    "VGB": "GBR",  # British Virgin Islands → UK
    "VIR": "USA",  # US Virgin Islands → USA
    "SXM": "NLD",  # Sint Maarten → Netherlands

    # Europe
    "ALD": "ALD",  # Alderney → part of Guernsey (map to GGY maybe)
    "AND": "AND",  # Andorra → itself
    "ESB": "ESP",  # Balearic Islands → Spain
    "GGY": "GBR",  # Guernsey → UK (or map as separate)
    "GIB": "GBR",  # Gibraltar → UK
    "IMN": "GBR",  # Isle of Man → UK
    "JEY": "GBR",  # Jersey → UK
    "LIE": "LIE",  # Liechtenstein → itself
    "SMR": "SMR",  # San Marino → itself
    "VAT": "VAT",  # Vatican → itself
    "NOR": "NOR",  # Svalbard/Jan Mayen → Norway
    "WSB": "WSB",  # Western Sahara → disputed, maybe None

    # Asia & Oceania
    "ASM": "USA",  # American Samoa → USA
    "ATC": "AUS",  # Ashmore and Cartier Islands → Australia
    "COK": "COK",  # Cook Islands → itself
    "COM": "COM",  # Comoros → itself
    "CPV": "CPV",  # Cape Verde → itself
    "CSI": "CSI",  # Channel Islands? → map separately
    "FSM": "FSM",  # Micronesia → itself
    "GUM": "USA",  # Guam → USA
    "HKG": "CHN",  # Hong Kong → China
    "HMD": "AUS",  # Heard Island and McDonald Islands → Australia
    "IOA": "GBR",  # British Indian Ocean Territory → UK
    "IOT": "GBR",  # same as above
    "KAB": "KAB",  # Kabardino-Balkaria (Russia) → maybe RUS
    "KAS": "KAS",  # Kashmir → disputed, maybe None
    "KIR": "KIR",  # Kiribati → itself
    "MAC": "CHN",  # Macau → China
    "MDV": "MDV",  # Maldives → itself
    "MHL": "MHL",  # Marshall Islands → itself
    "MLT": "MLT",  # Malta → itself
    "MNP": "USA",  # Northern Mariana Islands → USA
    "MUS": "MUS",  # Mauritius → itself
    "NFK": "AUS",  # Norfolk Island → Australia
    "NIU": "NIU",  # Niue → itself
    "NRU": "NRU",  # Nauru → itself
    "PCN": "PCN",  # Pitcairn Islands → itself
    "PGA": "PGA",  # Papua New Guinea? Check code
    "PLW": "PLW",  # Palau → itself
    "PYF": "FRA",  # French Polynesia → France
    "SGP": "SGP",  # Singapore → itself
    "SGS": "GBR",  # South Georgia → UK
    "SHN": "GBR",  # Saint Helena → UK
    "SOL": "SLB",  # Solomon Islands → SLB
    "SPM": "FRA",  # Saint Pierre and Miquelon → France
    "STP": "STP",  # Sao Tome and Principe → itself
    "TCA": "GBR",  # Turks and Caicos → UK
    "TON": "TON",  # Tonga → itself
    "TUV": "TUV",  # Tuvalu → itself
    "UMI": "USA",  # US Minor Outlying Islands → USA
    "USG": "USA",  # Guam? → USA
    "WLF": "WLF",  # Wallis and Futuna → itself

    "ESB": "ESP",
    "ALD": "AUS",
    "ANT": "NLD",
    "CLP": "CHL",
    "COK": "NZL",
    "CSI": "SLV",
    "CYN": "CYP",
    "KAB": "KAZ",
    "KAS": "KAZ",
    "PGA": "PNG",
    "PSX": "PSE",
    "SAH": "ESH",
    "SDS": "SDN",
    "WSB": "WSM",
}