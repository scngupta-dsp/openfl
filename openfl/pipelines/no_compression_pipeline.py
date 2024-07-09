"""NoCompressionPipeline module."""

from openfl.pipelines.pipeline import (
    Float32NumpyArrayToBytes,
    TransformationPipeline,
)


class NoCompressionPipeline(TransformationPipeline):
    """The data pipeline without any compression."""

    def __init__(self, **kwargs):
        """Initialize."""
        super(NoCompressionPipeline, self).__init__(
            transformers=[Float32NumpyArrayToBytes()], **kwargs)
