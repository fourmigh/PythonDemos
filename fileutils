import os
import zipfile

def delete_file_or_folder(root_path):
    if os.path.isdir(root_path):
        dir_or_files = os.listdir(root_path)
        for dir_file in dir_or_files:
            #获取目录或者文件的路径
            dir_file_path = os.path.join(root_path,dir_file)
            delete_file_or_folder(dir_file_path)
        os.rmdir(root_path)
        print("文件夹【" + root_path + "】已删除")
    elif os.path.isfile(root_path):
        os.remove(root_path)
        print("文件【" + root_path + "】已删除")
    else:
        print("【" + root_path + "】不是文件或文件夹")

def delete_and_mkdir(path):
    delete_file_or_folder(path)
    os.mkdir(path)
    print("【" + path + "】已创建")
    
def zip_files(files, zip_name):
    zp=zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
    for file in files:
        zp.write(file)
    zp.close()
    print('压缩完成')
    
if __name__ == '__main__':
    # path = 'E:\\PythonProjects\\Spyder\\temp'
    path = 'temp'
    # delete_file_or_folder(path)
    delete_and_mkdir(path)
