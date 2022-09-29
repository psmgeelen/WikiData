import logging
from typing import Union
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import numpy as np
import joblib
import time
from glob import glob
import os


class WikiData(object):

    def __init__(self, country_codes: Union[None, list] = None):

        if not os.path.exists('temp'):
            os.makedirs('temp')

        self.endpoint_url = "https://query.wikidata.org/sparql"
        self.country_codes = country_codes
        self.logger = self.start_logger_if_necessary()

    def get_continents_and_countries(self, write_csv: bool = False):
        """
        This method is focusses on getting the continents and countries out of the WikiData database. This first step can be executed generically, enabling the download of all countries that are listed on wikipedia. The results are routed within the class, but can be exported as a csv file for debugging purposes
        :param write_csv:
        :return: pd.DataFrame with the continents and countries
        """
        if self.country_codes is None:
            query = """SELECT ?country ?continent ?countryLabel ?continentLabel
                    WHERE
                    {
                      ?country  wdt:P31/wdt:P279* wd:Q6256;
                                wdt:P30 ?continent;
                      SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
                    }"""
            sparql = SPARQLWrapper(self.endpoint_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            data = sparql.query().convert()
            df = pd.json_normalize(data["results"]['bindings']).filter(regex=".*.value.*")
            df['country_codes'] = df['country.value'].str.split('/').str[-1]
            df['continent_codes'] = df['continent.value'].str.split('/').str[-1]

            if write_csv:
                df.to_csv('country_codes.csv')

            self.country_codes = df.loc[:, ['country_codes', 'countryLabel.value', 'continentLabel.value',
                                            'continent_codes']].to_dict(orient='records')

            return df
        else:
            exit(1)

    @staticmethod
    def _template_query(country_code: dict) -> str:
        return f""" SELECT ?country ?city ?continent ?population ?area ?countryLabel ?cityLabel ?continentLabel
                                WHERE
                                {{
                                  ?city wdt:P31/wdt:P279* wd:Q515;
                                        wdt:P17 wd:{country_code};
                                        wdt:P1082 ?population.
                                  OPTIONAL {{ ?city wdt:P2046 ?area . }}
                                  OPTIONAL {{ ?city wdt:P30 ?continent . }}
                                  OPTIONAL {{ ?city wdt:P1082 ?population. }}
                                  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
                                }}"""

    def _do_query(self, country_dict: dict) -> int:
        logger = self.start_logger_if_necessary()
        try:
            query = self._template_query(country_code=country_dict['country_codes'])
            sparql = SPARQLWrapper(self.endpoint_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            data = sparql.query().convert()
            df = pd.json_normalize(data["results"]['bindings']).filter(regex=".*.value.*")
            df['country_codes'] = country_dict['country_codes']
            df['countryLabel.value'] = country_dict['countryLabel.value']
            df['continentLabel.value'] = country_dict['continent_codes']
            df.to_csv(f'temp/{country_dict["country_codes"]}{country_dict["continent_codes"]}.csv')
            logger.info(f'Success with {country_dict["country_codes"]} {country_dict["continent_codes"]}')

            if os.path.exists(f'temp/{country_dict["country_codes"]}{country_dict["continent_codes"]}.failed'):
                os.remove(f'temp/{country_dict["country_codes"]}{country_dict["continent_codes"]}.failed')
                logger.info(
                    f'remove fail-file: temp/{country_dict["country_codes"]}{country_dict["continent_codes"]}.failed, as the data has been retrieved.')
            return 1

        except Exception as e:
            joblib.dump(country_dict, f'temp/{country_dict["country_codes"]}{country_dict["continent_codes"]}.failed')
            logger.info(
                f'Fail with  {country_dict["country_codes"]} {country_dict["continent_codes"]}, writing dictionary as Binary, exception: {e}')
            return 0

    def run_all_parallel(self, batchsize: int = 250, timer: int = 60) -> pd.DataFrame:
        """
        This method executes and re-executes the calls to the database of WikiData. The problem with WikiData is that there are many transcient errors, that require the proper management of failures. This method executes the calls per batch and writes successes as a CSV file to the temp folder, as pickles the jobs as *.failed when failing. After running through the entire set of jobs, the method re-iterates on the failed calls untill all calls are resolved with the requested data. The method returns a dataframe in which all the data is concatenated into a single file.
        :param batchsize: the size of the batch that is called upon simultaneously. Please note that the batchsize is also linked to the n_jobs parameter, meaning that a larger batch will spin up more processes and will consume more memory accordingly.
        :param timer: the pause between batches, which is recommended to be set to 60 seconds, as this is window in which resources are measured by WikiData.
        :return: resulting DataFrame.
        """
        if batchsize > len(self.country_codes):
            self.logger.info(
                f'Batchsize larger then set of items, adjusted from {batchsize} to {len(self.country_codes)}')
            batchsize = len(self.country_codes)

        lower = [item for item in range(0, len(self.country_codes), batchsize)]
        upper = [item + batchsize for item in range(0, len(self.country_codes), batchsize)]
        upper[-1] = len(self.country_codes)
        for batch in zip(lower, upper):
            self.logger.info(f'running batch {batch[0]} - {batch[1]}')
            with joblib.parallel_backend('loky'):
                success = joblib.Parallel(n_jobs=batchsize)(
                    joblib.delayed(self._do_query)(country_dict=country)
                    for country in self.country_codes[batch[0]:batch[1]])
            self.logger.info(f"The success-rate for batch {batch}, was {np.mean(success)}")
            self.logger.info(f"sleeping for {timer} seconds...")
            time.sleep(timer)

        new_batch = [joblib.load(item) for item in glob('temp/*.failed')]
        if len(new_batch) != 0:
            self.country_codes = new_batch
            self.run_all_parallel()
        else:
            self.logger.info("No failed files found, all item processed.")

        dfs = []
        for csv in glob("temp/*.csv"):
            dfs.append(pd.read_csv(csv))

        results = pd.concat(dfs, axis=0)
        return results

    def run_all_sync(self):
        results = []
        for country in self.country_codes:
            print(country)
            results.append(self._do_query(country_dict=country))
        return results

    @staticmethod
    def start_logger_if_necessary():
        logger = logging.getLogger("mylogger")
        if len(logger.handlers) == 0:
            logger.setLevel(logging.INFO)
            sh = logging.StreamHandler()
            sh.setFormatter(logging.Formatter("%(process)d %(threadName)s %(asctime)s %(levelname)-8s %(message)s"))
            fh = logging.FileHandler('out.log', mode='w')
            fh.setFormatter(logging.Formatter("%(process)d %(threadName)s %(asctime)s %(levelname)-8s %(message)s"))
            logger.addHandler(sh)
            logger.addHandler(fh)
        return logger
