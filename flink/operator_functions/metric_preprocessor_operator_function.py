from pyflink.datastream import RuntimeContext, ProcessFunction
from typing import Optional
from flink.operators.metric_preprocessor_operator import MetricPreprocessorOperator


class MetricPreprocessorOperatorFunction(ProcessFunction):
    def __init__(self):
        self.detector: Optional[MetricPreprocessorOperator] = None

    def open(self, runtime_context: RuntimeContext):
        self.detector = MetricPreprocessorOperator()

    def process_element(self, value, ctx: 'ProcessFunction.Context'):
        result = self.detector.run(value)
        if result:
            yield from result
