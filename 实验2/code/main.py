import os
import random
import struct
from shutil import copyfile
from faker import Faker

# 文件块大小
BLOCK_SIZE = 50
# 一条数据是24字节
# 内存中数据的数量
MEMORY_NUM = 200
# TOTAL_DATA小于MEMORY_SIZE^2/2,不然没法败者树，内存要溢出
TOTAL_DATA = 15000
BUFFER_NUM = 10
STORE_NUM = MEMORY_NUM - BUFFER_NUM
# GROUP_SIZE = round(TOTAL_DATA * 2 / STORE_NUM)
# GROUP_NUM = round(TOTAL_DATA / GROUP_SIZE)
GROUP_NUM = STORE_NUM
GROUP_SIZE = round(TOTAL_DATA / GROUP_NUM)
RECORD_SIZE = struct.calcsize("15s2i")
MAX = 1 << 30

FILE_PATH = "files/data"
OUT_PUT_PATH = "files/output"
fake = Faker()


def delete_data(path):
    if os.path.exists(path):
        os.remove(path)


def generate_data():
    """
    generate data
    :return:
    """
    with open(FILE_PATH, mode="wb+") as w:
        for i in range(TOTAL_DATA * BLOCK_SIZE):
            w.write(struct.pack("15s2i", bytearray(fake.name(), "utf-8"), i, random.randint(0, 1 << 20)))
            w.flush()
            print(f"\rhas generate {i} records", end='')
    print()
    print("-----------------------------")


def show_info():
    print("-----------------------------")
    print("external memory size(MB):", TOTAL_DATA * BLOCK_SIZE * 24 >> 20)
    print("memory size(kb):", MEMORY_NUM * BLOCK_SIZE * 24 >> 10)
    print("memory num:", MEMORY_NUM)
    print("total record size:", TOTAL_DATA * BLOCK_SIZE)
    print("group number:", GROUP_NUM)
    print("group size:", GROUP_SIZE)
    print("record size:", RECORD_SIZE)
    print("block size:", BLOCK_SIZE)
    print("-----------------------------")


def read_block(offset, path):
    """
    read data by block
    :param offset:
    :return:
    """
    tmp = []
    with open(path, mode="rb+") as r:
        r.seek(offset * BLOCK_SIZE * RECORD_SIZE)
        for i in range(BLOCK_SIZE):
            tmp.append(struct.unpack("15s2i", r.read(RECORD_SIZE)))
    return tmp


def write_block(offset, array, path):
    if len(array) > BLOCK_SIZE:
        print("array is overflow")
        exit(1)
    with open(path, mode="rb+") as w:
        w.seek(offset * BLOCK_SIZE * RECORD_SIZE)
        for i in array:
            w.write(struct.pack("15s2i", i[0], i[1], i[2]))


def first_sort():
    for i in range(GROUP_NUM):
        data = []
        for j in range(GROUP_SIZE):
            """处理结尾溢出的元组"""
            if i * GROUP_SIZE + j == TOTAL_DATA:
                data.sort(key=lambda x: x[2])
                for k in range(j):
                    write_block(i * GROUP_SIZE + k, get_block(k, data), FILE_PATH)
                print(f"\rhas sorted the {i} group", end='')
                return
            data += read_block(i * GROUP_SIZE + j, FILE_PATH)
        data.sort(key=lambda x: x[2])
        for j in range(GROUP_SIZE):
            write_block(i * GROUP_SIZE + j, get_block(j, data), FILE_PATH)
        print(f"\rhas sorted the {i} group", end='')
    print()
    print("-----------------------------")
    print()


def get_block(offset, array):
    if not len(array) % BLOCK_SIZE == 0:
        print("array is not block size")
        exit(1)
    return array[offset * BLOCK_SIZE:(offset + 1) * BLOCK_SIZE]


class LoserTree(object):
    def __init__(self, array):
        self.loser_tree_node = [0 for _ in array]
        self.nodes_count = len(array)
        self.leaves = array
        self.leaves.insert(0, -65535)
        for i in range(self.nodes_count, 0, -1):
            self.adjust(i)

    def adjust(self, index):
        k = self.nodes_count
        parent = (k - 1 + index) // 2
        while parent > 0:
            if self.leaves[index] > self.leaves[self.loser_tree_node[parent]]:
                # 值大者败
                # 进入此条件，说明leaves[index]是败者。所以对它的父节点进行赋值。
                self.loser_tree_node[parent], index = index, self.loser_tree_node[parent]
                # 交换后，index变成了优胜者
                # 求出parent的parent，进入下一轮循环。
            parent = parent // 2

        # 循环结束后，index一定是最后的优胜者，把最后的优胜者保存在ls[0]中。
        self.loser_tree_node[0] = index

    def get_winner_val(self):
        return self.leaves[self.loser_tree_node[0]]

    def winner_index(self):
        return self.loser_tree_node[0]

    def remove_winner(self, new_val):
        self.leaves[self.loser_tree_node[0]] = new_val
        self.adjust(self.loser_tree_node[0])


class Group:
    def __init__(self, members):
        self.members = members
        self.index = 0
        self.group_is_empty = False

    def get_member(self):
        return self.members.pop(0)

    def get_member_len(self):
        return len(self.members)

    def add_index(self):
        self.index += 1


def second_sort():
    with open(OUT_PUT_PATH, mode="wb+") as r:
        r.write(b"")
    segments = []
    for i in range(GROUP_NUM):
        segments.append(Group(read_block(i * GROUP_SIZE, FILE_PATH)))
    ls = LoserTree([i.members[0][2] for i in segments])
    block = []
    block_offset = 0
    while True:
        if len(block) == BLOCK_SIZE:
            write_block(block_offset, block, OUT_PUT_PATH)
            block_offset += 1
            block = []
            print(f"\rhas stored {block_offset} blocks", end='')
        if ls.get_winner_val() == MAX:
            break
        current_index = ls.winner_index() - 1
        current_group = segments[current_index]
        tmp_record = current_group.members.pop(0)
        block.append(tmp_record)
        if current_group.get_member_len() == 0:
            current_group.index += 1
            if current_group.index == GROUP_SIZE:
                current_group.group_is_empty = True
                ls.remove_winner(MAX)
            if current_index * GROUP_SIZE + current_group.index >= TOTAL_DATA:
                current_group.group_is_empty = True
                ls.remove_winner(MAX)
            if not current_group.group_is_empty:
                current_group.members = read_block(current_index * GROUP_SIZE + current_group.index, FILE_PATH)
        if not current_group.group_is_empty:
            ls.remove_winner(current_group.members[0][2])


def run():
    show_info()
    delete_data(OUT_PUT_PATH)
    delete_data(FILE_PATH)
    generate_data()
    first_sort()
    second_sort()


if __name__ == '__main__':
    run()
    print(read_block(12000, OUT_PUT_PATH))
