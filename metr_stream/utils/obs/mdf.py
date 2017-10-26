
from io import StringIO
from datetime import datetime


def strtodtype(dat_str):
    try:
        dat = int(dat_str.strip())
    except ValueError:
        try:
            dat = float(dat_str.strip())
        except ValueError:
            dat = dat_str.strip()
            dat = dat.strip('"')
    return dat


def dtypetostr(dat, width=None, prec=1):
    if type(dat) in [ str, unicode ]:
        if width is None:
            dat_str = dat
        else:
            dat_str = " " * (width - len(dat)) + dat
    elif type(dat) == float:
        if width is None:
            dat_str = "%.{fp:d}f".format(fp=prec)
        else:
            dat_str = "%{fw:d}.{fp:d}f".format(fw=width, fp=prec)
        dat_str = dat_str % dat
    elif type(dat) == int:
        if width is None:
            dat_str = "%{fw:d}d".format(fw=width)
        else:
            dat_str = "%d"
        dat_str = dat_str % dat
    return dat_str


class MDF(object):
    def __init__(self, base_time, cols, comment="", colwidths=None):
        self._base_time = base_time
        self._cols = cols
        self._comment = comment
        self._colwidths = colwidths if colwidths is not None else [ 6 ] * len(cols)
        self._format = 101

        self._rows = dict( (k, []) for k in cols )

    @classmethod
    def from_string(cls, text):
        sio = StringIO(text)
        header = sio.readline().strip()
        if '!' in header:
            fmt, comment = header.split('!')
        else:
            fmt = header
            comment = ''

        fmt = int(fmt.strip())
        comment = comment.strip()
        header = sio.readline()

        n_cols, base_date = header.strip().split(" ", 1)
        base_dt = datetime.strptime(base_date, "%Y %m %d %H %M %S")
        cols = sio.readline().strip().split()

        mdf = cls(base_dt, cols, comment=comment)
        for line in sio:
            mdf.appendrow(**dict(zip(cols, ( strtodtype(v) for v in line.strip().split() ))))

        return mdf

    def appendrow(self, **data):
        for k, v in data.items():
            self._rows[k].append(v)
        self._cols = list(self._rows.keys())

    def __getitem__(self, key):
        ret_val = None
        if type(key) in [ int, slice ]:
            ret_val = dict( (c, self._rows[c][key]) for c in self._rows.keys() )
        elif type(key) == str:
            ret_val = np.array(self._rows[key])
        return ret_val

    def __iter__(self):
        for idx in range(len(self)):
            yield self[idx]

    def __len__(self):
        return len(list(self._rows.values())[0])

    @property
    def columns(self):
        return list(self._rows.keys())

    @property
    def base_time(self):
        return self._base_time


