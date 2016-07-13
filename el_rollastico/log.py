import logging
import logging.config
import inspect

from el_rollastico import config

_CONFIGURED = False


def _configure():
    global _CONFIGURED
    if _CONFIGURED:
        return

    logging.config.dictConfig(config.LOGGING)

    _CONFIGURED = True


def _namespace_from_calling_context():
    """
    Derive a namespace from the module containing the caller's caller.

    :return: the fully qualified python name of a module.
    :rtype: str
    """
    # Not py3k compat
    # return inspect.currentframe(2).f_globals["__name__"]
    # TODO Does this work in both py2/3?
    return inspect.stack()[2][0].f_globals["__name__"]


def get_logger(name=None):
    _configure()

    if not name:
        name = _namespace_from_calling_context()

    return logging.getLogger(name)
