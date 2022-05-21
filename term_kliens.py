import socket


def main():
    address = ("localhost", 25567)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(address)
        reader = s.makefile('r')
        while True:
            snd = input('> ')
            if snd == 'exit':
                break

            s.sendall((snd + "\n").encode())

            buf = reader.readline()
            if 'ROWS' in buf:
                print(buf, end='')
                buf = reader.readline()
                while not buf.startswith('---'):
                    print(buf, end='')
                    buf = reader.readline()
            if len(buf) > 0:
                print(buf, end='')


if __name__ == '__main__':
    main()
