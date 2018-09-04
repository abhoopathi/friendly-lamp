
class A():
    _v1 = None
    def __init__(self):
        self.v1 = '5'
    def change(self,vals):
        self.v1 = vals
        return self.v1