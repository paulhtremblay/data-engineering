import os
import sys
import pytest

CUR_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(CUR_DIR)

import producer_generic

def test_producer_does_not_throw_errors():
    producer_generic.produce(
            topic_name = 'test', 
            key = 'test-key',
            sleep_time = .0001, 
            serializer = 'json', 
            data = 'words', 
            number = 1,
            verbose= False)

def test_producer_numbers_does_not_throw_errors():
    producer_generic.produce(
            topic_name = 'test', 
            key = 'test-key',
            sleep_time = .0001, 
            serializer = 'json', 
            data = 'numbers', 
            number = 1,
            verbose= False)

def test_producer_with_avro_does_not_throw_errors():
    producer_generic.produce(
            topic_name = 'test', 
            key = 'test-key',
            sleep_time = .0001, 
            serializer = 'avro', 
            data = 'words', 
            number = 1,
            verbose= False)

def test_producer_with_numbers_avro_does_not_throw_errors():
    producer_generic.produce(
            topic_name = 'test', 
            key = 'test-key',
            sleep_time = .0001, 
            serializer = 'avro', 
            data = 'numbers', 
            number = 1,
            verbose= False)

def test_producer_raises_error_wrong_topic():
    with pytest.raises(AssertionError) as exc_info:
        producer_generic.produce(
                topic_name = 'test', 
                key = 'test-key',
                sleep_time = .0001, 
                serializer = 'json', 
                data = 'bogus', 
                number = 1,
                verbose= False)
