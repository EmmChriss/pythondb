import datetime

def parser_input(input, type_input):
    if(type_input=='int'):
        try:
            int(input)
            return True
        except ValueError:
            return False
    elif (type_input=='float'):
        try:
            float(input)
            return True
        except ValueError:
            return False 
    elif (type_input=='string'):
        try:
            str(input)
            return True
        except ValueError:
            return False 
    elif (type_input=='bit'):
        if(input=='0' or input=='1'):
            return True
        else:
            return False
    elif (type_input=='date'):
        try:
            datetime.datetime.strptime(input, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    elif (type_input=='datetime'):
        try:
            datetime.datetime.strptime(input, '%Y-%m-%d-%H:%M:%S')
            return True
        except ValueError:
            return False
    else:
        return False