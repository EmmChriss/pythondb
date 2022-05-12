import datetime


def parser_input(input, type_input):
    try:
        match type_input:
            case 'int':
                int(input)
                return True
            case 'float':
                float(input)
                return True
            case 'string':
                str(input)
                return True
            case 'bit':
                return input in ['0', '1']
            case 'date':
                datetime.datetime.strptime(input, '%Y-%m-%d')
                return True
            case 'datetime':
                datetime.datetime.strptime(input, '%Y-%m-%d-%H:%M:%S')
                return True
    except ValueError:
        return False
    return False
