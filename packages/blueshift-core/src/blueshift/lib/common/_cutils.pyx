# cython: language_level=3
# distutils: define_macros=NPY_NO_DEPRECATED_API=NPY_1_7_API_VERSION
from blueshift.lib.common.constants import ALMOST_ZERO
from blueshift.lib.exceptions import ValidationError

cpdef check_input(object f, dict env):
    cdef str var
    cdef object check

    for var, check in f.__annotations__.items():
        val = env[var]
        if check.__class__ == type:
            if isinstance(val, check):
                return
            else:
                try:
                    check.__class__(val)
                    return
                except Exception:
                    pass
            msg = "Invalid argument {} in function {}: expected" \
                    "type {}".format(var,f.__name__,check)
            raise ValidationError(msg)
        elif callable(check):
            truth, msg = check(val)
            if truth:
                return
            raise ValidationError(msg.format(var,f.__name__))

cpdef cdict_diff(dict d1, dict d2):
    '''
        take difference of two dictions, treating the value as integer. it
        will list all keys (union of keys) in a new dict with values as the
        difference between the first and the second dict values, skipping
        zeros.
    '''
    cdef float value
    cdef dict diff = {}
    
    keys = set(d1.keys()).union(set(d2.keys()))
    try:
        for key in keys:
            value = float(d1.get(key,0)) - float(d2.get(key,0))
            if abs(value) > ALMOST_ZERO:
                diff[key] = value
    except OverflowError as e:
        msg = "error comparing dictionaries:"
        msg = msg + str(e)
        raise ValidationError(msg=msg)
    
    return diff