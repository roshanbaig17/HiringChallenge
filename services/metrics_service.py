import json
from services.kafka_service import KafkaService
import pandas as pd
from utilities import constants
import time
import logging

class MetricsService:

    def generate_metrics_from_kafka(self, group_id: str, seconds = 0, auto_offset_reset='latest'):
        """
        Consume stream from Kafka. Generate result after the interval passed as param 'second'.

        :param seconds: Interval after result will be genrated. Defauly 0 (i.e continous stream)
        :param auto_offset_reset: Offset from which data should start populating e.g latest, earliest

        :return: None
        """
        kafka_service = KafkaService()
        consumer = kafka_service.create_consumer_with_topic(topics=constants.KAFKA_CONSUMER_TOPICS,
                                                            server=constants.KAFKA_BOOTSTRAP_SERVER,
                                                            group_id=group_id,
                                                            auto_offset_reset=auto_offset_reset)
        df_list = []
        timeout = time.time() + seconds
        for data in kafka_service.consume_stream(consumer=consumer):
            df = self.__filter_stream(stream_json_object=data)
            if not df.empty:
                df_list.append(df)
            if time.time() > timeout:
                self.get_results(df_list=df_list)
                df_list = []
                timeout = time.time() + seconds

    def generate_metrics_from_json_string(self, json_str: str):
        """
        Accept string, convert it to json, filter required columns, create dataframe, and push results.
        Can be used to populate historical data from different source than Kafka.
        It can be used to get string input which contains json object for testing purposes.

        :param json_str: String containing json
        :return: None
        """

        json_obj = json.loads(json_str)
        df = self.__filter_stream(stream_json_object=json_obj)
        if not df.empty:
            self.get_results(df_list=[df])

    def get_results(self, df_list:[pd.DataFrame]):
        """
        Accept list of dataframe and generate metrics for per minute, week, month and year.
        Publish each metrics result.

        :param df_list: List of DF
        :return: None
        """
        result_df = pd.concat(df_list)
        result_df['date_time'] = pd.to_datetime(result_df['ts'], unit='s')

        # Per minute Metrics
        per_minute_df = self.__get_metrics_with_frequency(result=result_df, freq='1min')
        self.__publish_result(per_minute_df)

        # Week Metrics
        per_week_df = self.__get_metrics_with_frequency(result=result_df, freq='W')
        self.__publish_result(per_week_df)

        # Month Metrics
        per_month_df = self.__get_metrics_with_frequency(result=result_df, freq='M', dateFormat='%Y-%m')
        self.__publish_result(per_month_df)

        # Year Metrics
        per_year_df = self.__get_metrics_with_frequency(result=result_df, freq='Y', dateFormat='%Y')
        self.__publish_result(per_year_df)

    def __publish_result(self, df: pd.DataFrame):
        """
        Print result locally and call function to push data to Kafka stream.

        :param df: DataFrame
        :return: None
        """
        print(df.to_string())
        self.__push_data_stream(df)

    def __get_metrics_with_frequency(self, result: pd.DataFrame, freq: str, dateFormat='%Y-%m-%d %H:%M:%S') -> pd.DataFrame:
        """
        Group the result dataframe based on date_time and get unique id count.

        :param result: DataFrame on which actions will be performed
        :param freq: Frequency i.e min, week, month, year
        :param dateFormat: Dateformat to add formatted_date_time to dataframe

        :return: DataFram
        """
        grouped_df = result.groupby(pd.Grouper(key='date_time', freq=freq)).uid.unique().reset_index()
        grouped_df.columns = ['date_time', 'uid']
        grouped_df['unique_users_count'] = grouped_df['uid'].str.len()
        grouped_df['formatted_date_time'] = grouped_df['date_time'].dt.strftime(dateFormat)
        return grouped_df

    def __push_data_stream(self, df: pd.DataFrame):
        """
        Convert dataframe to json and produce to Kafka topic.

        :param df: Dataframe to produce to Kafka
        :return:
        """
        kafka_service = KafkaService()
        isSuccess = kafka_service.produce_stream(constants.KAFKA_BOOTSTRAP_SERVER, constants.KAFKA_PRODUCER_TOPIC, df.to_json())
        if not isSuccess:
            logging.error('MetricsService | Producer | Failed to publish data')

    def __filter_stream(self, stream_json_object) -> pd.DataFrame:
        """
        Filter required data and return dataframe.

        :param stream_json_object: Dictionary
        :return: Dataframe
        """
        if not 'ts' in stream_json_object:
            logging.error('MetricsService | Missing ts in the json object')
            return pd.DataFrame()
        elif not 'uid' in stream_json_object:
            logging.error('MetricsService | Missing uid in the json object')
            return pd.DataFrame()
        final_json_object = {'ts': stream_json_object['ts'], 'uid': stream_json_object['uid']}
        df = pd.DataFrame(final_json_object, index=[0])
        return df