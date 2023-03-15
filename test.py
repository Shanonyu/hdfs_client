def split_with_quotes(string: str) -> list[str]:

    tmp = ""
    result = []
    l = len(string) - 1
    i = 0

    while (i < l - 1):
        if string[i] == " ":
            i += 1
            if string[i] == '"':
                i += 1
                while (string[i] != '"' and i < l):
                    tmp += string[i]
                    i += 1
                result.append(tmp)
                tmp = ""
                i += 1
            else:
                while (string[i] != ' ' and i < l):
                    tmp += string[i]
                    i += 1
                result.append(tmp)
                tmp = ""
        else:
            while (string[i] != ' ' and i < l):
                tmp += string[i]
                i += 1
            result.append(tmp)
            tmp = ""
    return result

a = 'command "First arg" "Second arg" sex penis "another one"'
b = split_with_quotes(a)
print(b)
