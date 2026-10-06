class EndOfDayPipelineError(RuntimeError):
    pass


class PricingDataIntegrityError(EndOfDayPipelineError):
    pass


class AlpacaDownloadIntegrityError(EndOfDayPipelineError):
    pass
