import binascii
if __name__ == '__main__':
    with open('metadata.txt') as in_file, open('out', 'wb') as out_file:
        lines = in_file.readlines()
        payload_lines = (x[0:2] for x in lines if len(x) == 3)
        streeeng = "".join(payload_lines)
        hex = binascii.unhexlify(streeeng)
        out_file.write(hex)
