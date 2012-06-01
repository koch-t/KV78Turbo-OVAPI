# TODO: this class needs to handle escaped '|'

import simplejson

class ctxtable:
    def __init__(self, Label, Name = None, Comment = None, Path = None, Endian = None, Enc = None, Res1 = None):
        self.Label = Label
        self.Name = Name
        self.Comment = Comment
        self.Path = Path
        self.Endian = Endian
        self.Enc = Enc
        self.Res1 = Res1

        self.fieldlabels = []
        self.values = []

    def set_fieldlabels(self, fieldlabels):
        self.fieldlabels = fieldlabels

    def dict(self):
        return {'meta': {'label': self.Label, 'name': self.Name, 'comment': self.Comment, 'path': self.Path, 'endian': self.Endian, 'enc': self.Enc, 'res1': self.Res1}, 'fieldlabels': self.fieldlabels, 'values': self.values}

    def columns(self):
        result = {}
        for x in range(0, len(self.fieldlabels)):
            result[self.fieldlabels[x]] = [y[x] for y in self.values]

        return result

    def rows(self, empty=False):
        results = []
        for y in self.values:
            result = {}
            for x in range(0, len(self.fieldlabels)):
                if not empty or y[x] is not None:
                    result[self.fieldlabels[x]] = y[x]
            if len(result) > 0:
                results.append(result)    
        return results
    
    def rowsdict(self, keys):
        results = {}
        for y in self.values:
            result = {}
            for x in range(0, len(self.fieldlabels)):
                if y[x] is not None:
                    result[self.fieldlabels[x]] = y[x]
            if len(result) > 0:
                key = '|'.join([result[x] for x in keys])
                results[key] = result
        return results

    def append(self, result):
        tmp = []
        for x in result:
            if x == '\\0':
                tmp.append(None)
            else:
                tmp.append(x)
             
        self.values.append(tmp)

    def __repr__(self):
        return str(self.Label)

class ctx:
    def __init__(self, content = None):
        self.ctx = {}
        if content is not None:
            self._content = content
            self.parse()

    def parse(self):
        tLabel = None

        for line in self._content.split('\r\n')[:-1]:
            if line[0] == '\\':
                # control characters
                if line[1] == 'G':
                    Label, Name, Comment, Path, Endian, Enc, Res1, TimeStamp, _ = line[2:].split('|')
                elif line[1] == 'T':
                    tmp = ctxtable(*line[2:].split('|'))
                    tLabel = tmp.Label
                    self.ctx[tLabel] = tmp
                elif line[1] == 'L':
                    self.ctx[tLabel].set_fieldlabels(line[2:].split('|'))

            else:
                self.ctx[tLabel].append(line.split('|'))


#contents = open('/tmp/KV8/1331344996.79').read()
#c = ctx(contents)
#print c.ctx['DATEDPASSTIME'].rows()
