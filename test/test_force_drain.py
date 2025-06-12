import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam import Create, ParDo, pvalue
from apache_beam.testing.util import assert_that, equal_to

import sys
import os

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(parent_dir)


from pipeline import ForceDrainFn


class TestForceDrainFn(unittest.TestCase):
    def test_force_drain(self):
        with TestPipeline() as p:
            input_data = p | "CreateInput" >> Create(["done"])

            results = input_data | "RunForceDrainFn" >> ParDo(ForceDrainFn()).with_outputs("drain_done", main="main")


            assert_that(results.main, equal_to([]), label="CheckMainOutputIsEmpty")

            assert_that(results.drain_done, equal_to([True]), label="CheckDrainDoneOutput")


if __name__ == "__main__":
    unittest.main()