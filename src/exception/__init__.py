import sys

def error_message_detail(error, error_detail:sys):
    _, _, exc_tb = error_detail.exc_info()
    filename = exc_tb.tb_frame.f_code.co_filename
    error_message = f"\nError occured in python script {filename} line no {exc_tb.tb_lineno} error message {error}"
    return error_message

class UserException(Exception):
    def __init__(self, error_message, error_detail:sys):
        self.error_message = error_message_detail(error_message, error_detail)
        super().__init__(self.error_message)
