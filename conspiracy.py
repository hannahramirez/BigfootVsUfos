import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import lxml


def scrape_population():
    # obtained dictionary from https://gist.github.com/rogerallen/1583593
    us_state_to_abbrev = {
        "Alabama": "AL",
        "Alaska": "AK",
        "Arizona": "AZ",
        "Arkansas": "AR",
        "California": "CA",
        "Colorado": "CO",
        "Connecticut": "CT",
        "Delaware": "DE",
        "Florida": "FL",
        "Georgia": "GA",
        "Hawaii": "HI",
        "Idaho": "ID",
        "Illinois": "IL",
        "Indiana": "IN",
        "Iowa": "IA",
        "Kansas": "KS",
        "Kentucky": "KY",
        "Louisiana": "LA",
        "Maine": "ME",
        "Maryland": "MD",
        "Massachusetts": "MA",
        "Michigan": "MI",
        "Minnesota": "MN",
        "Mississippi": "MS",
        "Missouri": "MO",
        "Montana": "MT",
        "Nebraska": "NE",
        "Nevada": "NV",
        "New Hampshire": "NH",
        "New Jersey": "NJ",
        "New Mexico": "NM",
        "New York": "NY",
        "North Carolina": "NC",
        "North Dakota": "ND",
        "Ohio": "OH",
        "Oklahoma": "OK",
        "Oregon": "OR",
        "Pennsylvania": "PA",
        "Rhode Island": "RI",
        "South Carolina": "SC",
        "South Dakota": "SD",
        "Tennessee": "TN",
        "Texas": "TX",
        "Utah": "UT",
        "Vermont": "VT",
        "Virginia": "VA",
        "Washington": "WA",
        "West Virginia": "WV",
        "Wisconsin": "WI",
        "Wyoming": "WY",
        "District of Columbia": "DC",
        "American Samoa": "AS",
        "Guam": "GU",
        "Northern Mariana Islands": "MP",
        "Puerto Rico": "PR",
        "United States Minor Outlying Islands": "UM",
        "U.S. Virgin Islands": "VI",
        "United States": "US"
    }
    # visiting the page and getting the html
    url = "https://en.wikipedia.org/wiki/List_of_U.S._states_and_territories_by_historical_population"
    page = requests.get(url)

    # checking status to see if it is safe to scrape
    # print(page.status_code)

    # class that contains all the population data i need
    # i identified this class using the inspect tool in google and inspecting the table
    html_class = "wikitable"
    soup = BeautifulSoup(page.content, "html.parser")

    # there are multiple tables on the page that break up all the population data
    # i will keep all the tables in a list and them later combine them
    list_df = []
    with open("state_pop_decade.csv", "w") as f_out:
        # finds each table on the page and iterates through one at a time
        for t in soup.find_all("table", attrs={"class": html_class}):
            # converts beautiful soup data to a string
            html = str(t)

            # there were a lot of citations in the format of [x]
            # I use the following regex expression to find & remove all citations
            html = re.sub("\[(.*?)\]", "", html)

            # pandas has a built in functions that converts html tables
            df = pd.read_html(html)
            df = pd.DataFrame(df[0])

            # i put all tables into a list
            list_df.append(df)

        # COMBINING DATAFRAMES
        # Pandas's merge function requires at least one common columun
        # so I start my final dataframe with the first dataframe
        main_df = list_df[0]
        # I iterate through the rest of the dataframes and merge to the main one
        for i in range(1, len(list_df) - 4):
            main_df = main_df.merge(list_df[i], "right")

        # I renamed name column to state just to be more specific
        main_df["State"] = main_df.apply(lambda row: us_state_to_abbrev[row.Name], axis=1)
        state_col = main_df.pop("State")
        main_df.insert(1, "State", state_col)
        # I drop admitted column because it's empty and I do not need it.
        main_df = main_df.drop(["Admitted"], axis=1)
        # I convert the final dataframe to a csv file for easy reference and to save information
        main_df.to_csv(f_out, index=False)


# the following methods are
def ufo():
    with open("ufo.csv", 'r') as fin:
        with open("ufo_mod.csv", 'w') as fout:
            # read data from csv file
            df = pd.DataFrame(pd.read_csv(fin))

            # filtering out columns i do not need
            columns = set(df.columns)
            want = {'0', '1', '2', '3', '8', '9', '10'}
            columns = columns - want
            [df.pop(name) for name in columns]

            # renaming columns
            df = df.rename(
                columns={'0': "date", "1": "city", '2': "state", '3': "country", "8": "report", '9': "latitude",
                         "10": "longitude"})

            # filtering out any records that did not take place in the us
            df = df[df.country == 'us']

            # date handling and decade bining
            list = df['date'].str.split(" ", expand=True)
            df.pop("date")
            df["date"] = list[0]

            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')

            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day

            df['decade'] = df['year'] - df['year'] % 10

            # population normalization value
            with open("state_pop_decade.csv", 'r') as pop_file:
                population = pd.DataFrame(pd.read_csv(pop_file))
                population = population.set_index("State")

                def calculate(row):
                    pop = population.at[row["state"].upper(), str(row["decade"])]
                    if pop == 0:
                        return None
                    else:
                        return 1 / pop

                df['norm_population'] = df.apply(calculate, axis=1)

            # write the modified csv file
            df.to_csv(fout, index=False)


def bigfoot():
    with open("bigfoot.csv", 'r') as fin:
        with open("bigfoot_mod.csv", 'w') as fout:
            # read in csv file
            df = pd.DataFrame(pd.read_csv(fin))

            # filtering out any data points that did not have the date attached
            df = df.dropna(subset="date")

            # filtering out unneeded columns
            columns = set(df.columns) - {"county", "state", "latitude", "longitude", "date"}
            [df.pop(name) for name in columns]

            # date handling and decade bining
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day'] = df['date'].dt.day

            df['decade'] = df['year'] - df['year'] % 10

            # population normalization
            with open("state_pop_decade.csv", 'r') as pop_file:
                population = pd.DataFrame(pd.read_csv(pop_file))
                population = population.set_index("Name")

                def calculate(row):
                    pop = population.at[row["state"], str(row["decade"])]
                    if pop == 0:
                        return None
                    else:
                        return 1 / pop

                df['norm_population'] = df.apply(calculate, axis=1)

            # write the modified csv file
            df.to_csv(fout, index=False)


if __name__ == "__main__":
    # step 1
    # scrape_population()
    # step 2
    ufo()
    bigfoot()
