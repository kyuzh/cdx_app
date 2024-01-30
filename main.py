# 导入所需的库
import re
from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
import time
from selenium.webdriver.edge.options import Options
import tkinter as tk
from tkinter import ttk, filedialog
from tkinter import messagebox
import warnings


# Ignorer tous les avertissements
warnings.filterwarnings("ignore")

def merge_csv():
    main_file = main_file_entry_3.get()
    sub_file = sub_file_entry_3.get()
    output_file = output_file_entry_3.get()

    if main_file and sub_file and output_file:
        try:
            merge_csv_files(main_file, sub_file, output_file)
            result_label.config(text="Fusion terminée avec succès!")
        except Exception as e:
            result_label.config(text=f"Erreur : {str(e)}")
    else:
        result_label.config(text="Veuillez sélectionner les fichiers et spécifier un fichier de sortie.")


def start_web_scrap():
    path = path_entry.get() + "/"
    file_name = file_name_entry.get()
    page_start = int(page_start_entry.get())
    page_end = int(page_end_entry.get())
    print(path+file_name)
    try:
        web_scrap_monreseau(path, file_name, page_start, page_end)
        messagebox.showinfo("Succès", "Extraction des données terminée avec succès!")
    except Exception as e:
        messagebox.showerror("Erreur", f"Une erreur s'est produite : {str(e)}")

def browse_folder(path_entry):
    folder_path = filedialog.askdirectory()  # Ouvrir une boîte de dialogue pour sélectionner un dossier
    path_entry.delete(0, tk.END)  # Effacez le contenu actuel de l'entrée path_entry
    path_entry.insert(0, folder_path)  # Insérez le chemin du dossier sélectionné dans l'entrée

def browse_file_name(file_name_entry):
    file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
    if file_path:
        file_name_entry.delete(0, tk.END)
        file_name_entry.insert(0, file_path)


# Fonction pour appeler get_open_data
def get_open_data_interface():
    # Récupérer les valeurs des entrées
    path_input = path_input_entry.get() + "/"
    path_output = path_output_entry.get() + "/"
    file_result_part1 = file_result_part1_entry.get()
    outputfile_name = outputfile_name_entry.get()

    # Appeler la fonction get_open_data avec les valeurs récupérées
    try:
        get_open_data(path_input, path_output, file_result_part1, outputfile_name)
        messagebox.showinfo("Succès", "Extraction des données terminée avec succès!")
    except Exception as e:
        messagebox.showerror("Erreur", f"Une erreur s'est produite : {str(e)}")

def merge_csv_files(main_file, sub_file, output_file):
    # Charger les deux fichiers CSV dans des DataFrames
    df1 = pd.read_csv(main_file, header=0)
    df2 = pd.read_csv(sub_file, header=0)

    # Concatenate the two DataFrames along the columns axis
    merged_df = pd.concat([df1, df2])
    # Supprimer les doublons en fonction de la colonne 'siret'
    merged_df = merged_df.drop_duplicates(subset='siret')
    # Enregistrer le résultat fusionné dans un nouveau fichier CSV
    merged_df.to_csv(output_file, index=False)

def get_open_data(path_input, path_output, file_result_part1, outputfile_name):
    filename1 = path_input + 'StockEtablissement_utf8.csv'
    filename2 = path_input + 'StockEtablissementHistorique_utf8.csv'
    filename3 = path_input + 'StockEtablissementLiensSuccession_utf8.csv'
    filename4 = path_input + 'StockUniteLegale_utf8.csv'
    filename5 = path_input + 'StockUniteLegaleHistorique_utf8.csv'

    list_filename = [filename1, filename2, filename3, filename4, filename5]

    # Parcours des fichiers de la liste list_filename
    for file in [filename1, filename4]:
        df = ""
        chunksize = 10 ** 6
        count = 0
        k = 1
        for chunk in pd.read_csv(file, chunksize=chunksize):
            if isinstance(df, str):
                df = chunk
            else:
                df = pd.concat([df, chunk], axis=0)
            count += 1
            if count == 5:
                df.to_csv(file[:-4] + "_" + str(k) + ".csv", index=False)
                df = ""
                count = 0
                print("decouper" + file + "K: " + k)
                k += 1

    # Chargement des fichiers CSV générés précédemment dans des DataFrames
    filename1_new = filename1.replace("_utf8", "_utf8_1")
    print("lire StockEtablissement_utf8.csv")
    df11 = pd.read_csv(filename1_new)
    df12 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_2"))
    df13 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_3"))
    df14 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_4"))
    df15 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_5"))
    df16 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_6"))
    df17 = pd.read_csv(filename1_new.replace("_utf8_1", "_utf8_7"))
    list_df1 = [df11, df12, df13, df14, df15, df16, df17]

    list_column = ['siren', 'siret', 'numeroVoieEtablissement', 'indiceRepetitionEtablissement',
                   'typeVoieEtablissement', 'libelleVoieEtablissement',
                   'codePostalEtablissement', 'libelleCommuneEtablissement']
    list_adress = ['numeroVoieEtablissement', 'indiceRepetitionEtablissement',
                   'typeVoieEtablissement', 'libelleVoieEtablissement']

    # Sélection des colonnes souhaitées pour chaque DataFrame dans list_df1
    for i in range(len(list_df1)):
        list_df1[i] = list_df1[i][list_column]

    # Conversion des colonnes d'adresse en chaînes de caractères
    for df in list_df1:
        for col in list_adress:
            df[col] = df[col].astype(str)

    # Création de la colonne 'AdressEtablissement' en fusionnant les colonnes d'adresse
    for df in list_df1:
        df['AdressEtablissement'] = df[list_adress].agg(' '.join, axis=1)

    list_column.append('AdressEtablissement')

    # Nettoyage de la colonne 'AdressEtablissement'
    for df in list_df1:
        df['AdressEtablissement'] = df['AdressEtablissement'].apply(lambda x: x.replace("nan", ""))
        df['AdressEtablissement'] = df['AdressEtablissement'].apply(lambda x: x.replace("  ", " "))

    df_all = ""
    # Concaténation des DataFrames dans list_df1 pour former un DataFrame unique
    for df in list_df1:
        if isinstance(df_all, str):
            df_all = df[
                ['siret', 'siren', 'AdressEtablissement', 'codePostalEtablissement', 'libelleCommuneEtablissement']]
        else:
            df_all = pd.concat([df_all, df[
                ['siret', 'siren', 'AdressEtablissement', 'codePostalEtablissement', 'libelleCommuneEtablissement']]],
                               axis=0)
    print("lire" + file_result_part1)
    df_result_part_1 = pd.read_csv(file_result_part1)

    # Fusion du DataFrame df_all avec df_test sur la colonne 'siren'
    df_resul = df_result_part_1.merge(df_all, how='left', on=['siren'])

    print("lire StockUniteLegale_utf8.csv")
    df41 = pd.read_csv(filename4.replace("_utf8", "_utf8_1"))
    df42 = pd.read_csv(filename4.replace("_utf8", "_utf8_2"))
    df43 = pd.read_csv(filename4.replace("_utf8", "_utf8_3"))
    df44 = pd.read_csv(filename4.replace("_utf8", "_utf8_4"))
    df45 = pd.read_csv(filename4.replace("_utf8", "_utf8_5"))
    list_df4 = [df41, df42, df43, df44, df45]
    df_all4 = ""

    # Concaténation des DataFrames dans list_df4 pour former un DataFrame unique
    for df in list_df4:
        if isinstance(df_all4, str):
            df_all4 = df[['siren', 'prenom1UniteLegale', 'nomUniteLegale']]
        else:
            df_all4 = pd.concat([df_all4, df[['siren', 'prenom1UniteLegale', 'nomUniteLegale']]], axis=0)

    # Fusion du DataFrame df_all4 avec df_resul sur la colonne 'siren'
    df = df_resul.merge(df_all4, how='left', on=['siren'])
    # Enregistrement du DataFrame fusionné dans un fichier CSV
    df.to_csv(path_output + outputfile_name, index=False)
    print("Finish")

def web_scrap_monreseau(path, file_name, page_start, page_end):
    # 定义用户代理字符串
    user_ag = [
        'MQQBrowser/26 Mozilla/5.0 (Linux; U; Android 2.3.7; zh-cn; MB200 Build/GRJ22; ' + 'CyanogenMod-7) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1',
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) " + "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36"]

    user_ag2 = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3493.3 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36"]

    # 设置浏览器选项
    options = Options()
    options.add_argument('user-agent=%s' % user_ag[0])
    options.add_argument("--headless")
    options.add_argument("--disable-infobars")
    options.add_argument('blink-settings=imagesEnabled=false')
    # 使用配置的选项初始化 Edge WebDriver 实例
    driver = webdriver.Edge(options=options)

    url = "https://www.monreseau-it.fr/liste-revendeurs-it.htm#page=1&mshact=48,49,50,51,52,53,54,55,56,57,58,59,60,61"

    # 获取最大页数
    driver.get(url)
    time.sleep(5)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    driver.quit()
    nb_societe = soup.find("span", {"class": "blue"}).text
    nb_societe = int(nb_societe.replace("sociétés", "").replace(" ", ""))
    nb_page = int(nb_societe / 25 + 0.99)
    print("nb_page:", nb_page)
    if page_end > nb_page:
        page_end = nb_page
    # 爬取每页数据
    for page in range(page_start, page_end+1):

        list_compag_url = []
        list_compag_siren = []
        list_compag_name = []
        list_type = []
        list_nb_Effectif = []
        list_nb_CF = []
        list_grossistes_list = []

        # 设置浏览器选项
        options = Options()
        # 修改用户代理
        options.add_argument('user-agent=%s' % user_ag[page % 2])
        options.add_argument("--headless")
        options.add_argument("--disable-infobars")
        options.add_argument('blink-settings=imagesEnabled=false')
        # 初始化 Edge WebDriver 实例 更改用户代理
        driver = webdriver.Edge(options=options)

        print("page:", page)
        # 构建 URL
        url = "https://www.monreseau-it.fr/liste-revendeurs-it.htm#page=" + str(
            page) + "&mshact=48,49,50,51,52,53,54,55,56,57,58,59,60,61"

        # 加载 URL
        driver.get(url)
        time.sleep(4)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        # 关闭 WebDriver 实例
        driver.quit()

        # 提取公司名称
        list_compag = soup.find_all("a", href=re.compile("revendeur/"))
        list_compag_url = list(list_compag)
        list_compag_name = list_compag_name + [str(list_compag[i].text) for i in range(len(list_compag))]

        # 计数
        count = 0
        for compag_url in list_compag_url:
            # 增加计数
            count += 1
            print("count:", count)
            # 将compag_url转换为字符串
            compag_url = str(compag_url)

            # 提取href链接的起始和结束索引
            beg_href_compag_url = compag_url.index('href="') + 6
            end_href_compag_url = compag_url.rindex('.htm') + 4
            # 构建完整的href链接
            href = "https://www.monreseau-it.fr/" + compag_url[beg_href_compag_url: end_href_compag_url]
            print(href)
            # 提取SIREN号并添加到list_compag_siren列表中
            list_compag_siren.append(href[-13:-4])
            # 设置浏览器选项
            options = Options()
            # 使用不同的用户代理
            options.add_argument('user-agent=%s' % user_ag2[count % 2])
            options.add_argument("--headless")
            options.add_argument("--disable-infobars")
            options.add_argument('blink-settings=imagesEnabled=false')
            # 初始化 Edge WebDriver 实例 更改用户代理
            driver = webdriver.Edge(options=options)
            # 加载公司URL
            driver.get(href)
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            driver.quit()

            # 获取公司类型数据
            type = str(soup.find("div", class_="span9").h2)[4:-5]
            # 获取关键数字数据
            chiffre_cle = soup.find("table", class_="table table-bordered")
            if "<td>" in str(chiffre_cle):
                chiffre_cle = chiffre_cle.find_all("td")
                nb_Effectif = str(chiffre_cle[0])[4:-13]
                if len(chiffre_cle) == 2:
                    nb_CF = str(chiffre_cle[1])[4:-5]
                else:
                    nb_CF = " "
            else:
                nb_Effectif = " "
                nb_CF = " "
            # 获取批发商数据
            grossistes = soup.find("div", id="grossistes")
            if "li" in str(grossistes):
                grossistes = grossistes.find_all("li")
                grossistes_list = [str(grossiste)[str(grossiste).index('">') + 2:-9] for grossiste in grossistes]
            else:
                grossistes_list = " "

            print(type, nb_Effectif, nb_CF, grossistes_list)
            # 将数据添加到列表中
            list_type.append(type)
            list_nb_Effectif.append(nb_Effectif)
            list_nb_CF.append(nb_CF)
            list_grossistes_list.append(grossistes_list)

        print(list_compag_name, list_compag_siren)
        # 创建另一个DataFrame对象并将其保存到CSV文件中
        df = pd.DataFrame({'nom entreprise': list_compag_name, "siren": list_compag_siren, 'type': list_type,
                           'nb Effectif': list_nb_Effectif, "Chiffre d'Affaire": list_nb_CF,
                           "Grossiste": list_grossistes_list})
        df.to_csv(path + file_name, mode='a', header=True, index=False)

# Fonction pour afficher l'interface principale
def show_main_interface():
    # Affichez le cadre principal
    main_frame.grid(row=1, column=0, columnspan=3, padx=20, pady=20)
    # Masquez les autres cadres
    first_option_frame.grid_forget()
    second_option_frame.grid_forget()
    third_option_frame.grid_forget()

# Fonction pour afficher l'interface du premier option
def show_first_option_interface():
    # Masquez le cadre principal
    main_frame.grid_forget()
    # Affichez le cadre de la première option
    first_option_frame.grid(row=1, column=0, columnspan=3, padx=20, pady=20)
    # Masquez le cadre de la deuxième option
    second_option_frame.grid_forget()
    # Masquez le cadre de la troisième option
    third_option_frame.grid_forget()

# Fonction pour afficher l'interface de la deuxième option
def show_second_option_interface():
    # Masquez le cadre principal
    main_frame.grid_forget()
    # Masquez le cadre de la première option
    first_option_frame.grid_forget()
    # Affichez le cadre de la deuxième option
    second_option_frame.grid(row=1, column=0, columnspan=3, padx=20, pady=20)
    # Masquez le cadre de la troisième option
    third_option_frame.grid_forget()

# Fonction pour afficher l'interface de la troisième option
def show_third_option_interface():
    # Masquez le cadre principal
    main_frame.grid_forget()
    # Masquez le cadre de la première option
    first_option_frame.grid_forget()
    # Masquez le cadre de la deuxième option
    second_option_frame.grid_forget()
    # Affichez le cadre de la troisième option
    third_option_frame.grid(row=1, column=0, columnspan=3, padx=20, pady=20)


# Créer la fenêtre principale
root = tk.Tk()
root.title("Web Scraping Interface")
root.geometry("600x400")  # Taille de la fenêtre

# Créer un style pour les boutons
style = ttk.Style()
style.configure('TButton', font=('Helvetica', 12))


# Créer un cadre principal
"""     interface principale       """
main_frame = ttk.Frame(root, padding=10)
main_frame.grid(row=1, column=0, columnspan=3, padx=20, pady=20)

# Créer un cadre pour le premier option
first_option_frame = ttk.Frame(root, padding=10)

# Créer un cadre pour le deuxième option
second_option_frame = ttk.Frame(root, padding=10)

# Créer un cadre pour le troisième option
third_option_frame = ttk.Frame(root, padding=10)

# Créer et ajouter des widgets à l'interface principale (dans le cadre principal)
ttk.Label(main_frame, text="Menu", font=('Helvetica', 16)).grid(column=0, row=0, columnspan=3, pady=20)
ttk.Button(main_frame, text="Web scrab MonRéseau IT", style='TButton', command=show_first_option_interface).grid(column=0, row=1, pady=10)
ttk.Button(main_frame, text="Collection des données", style='TButton', command=show_second_option_interface).grid(column=1, row=1, pady=10)
ttk.Button(main_frame, text="Fusion", style='TButton', command=show_third_option_interface).grid(column=2, row=1, pady=10)


"""     interface 1er option       """
# Créer et ajouter des widgets à l'interface de la deuxième option (dans le cadre de la deuxième option)
ttk.Label(first_option_frame, text="Web scrab MonRéseau IT", font=('Helvetica', 16)).grid(column=0, row=0, columnspan=3, pady=20)

# Créer et ajouter des widgets au cadre du premier option
ttk.Label(first_option_frame, text="Chemin du fichier de sortie:", font=('Helvetica', 14)).grid(column=0, row=1, sticky=tk.W, pady=10)
path_entry = ttk.Entry(first_option_frame)
path_entry.grid(column=1, row=1)
# Créez un bouton pour choisir le dossier
browse_button = ttk.Button(first_option_frame, text="Sélectionner un dossier", style='TButton', command=lambda: browse_folder(path_entry))
browse_button.grid(column=2, row=1, pady=10)

ttk.Label(first_option_frame, text="Nom du fichier de sortie:", font=('Helvetica', 14)).grid(column=0, row=2, sticky=tk.W, pady=10)
file_name_entry = ttk.Entry(first_option_frame)
file_name_entry.grid(column=1, row=2)

ttk.Label(first_option_frame, text="Page de début:", font=('Helvetica', 14)).grid(column=0, row=3, sticky=tk.W, pady=10)
page_start_entry = ttk.Entry(first_option_frame)
page_start_entry.grid(column=1, row=3)

ttk.Label(first_option_frame, text="Page de fin:", font=('Helvetica', 14)).grid(column=0, row=4, sticky=tk.W, pady=10)
page_end_entry = ttk.Entry(first_option_frame)
page_end_entry.grid(column=1, row=4)

start_button = ttk.Button(first_option_frame, text="Démarrer l'extraction", style='TButton', command=start_web_scrap)
start_button.grid(column=0, row=5, columnspan=3, pady=20)
# Créer et ajouter des widgets à l'interface de la deuxième option (dans le cadre de la deuxième option)
ttk.Button(first_option_frame, text="Retour à l'interface principale", style='TButton', command=show_main_interface).grid(column=0, row=6, columnspan=3, pady=20)

"""     interface 2e option       """

# Créer et ajouter des widgets à l'interface de la deuxième option (dans le cadre de la deuxième option)
ttk.Label(second_option_frame, text="Collection des données", font=('Helvetica', 16)).grid(column=0, row=0, columnspan=3, pady=20)

# Entrées pour les paramètres de get_open_data
ttk.Label(second_option_frame, text="Chemin d'entrée:", font=('Helvetica', 14)).grid(column=0, row=1, sticky=tk.W, pady=10)
path_input_entry = ttk.Entry(second_option_frame)
path_input_entry.grid(column=1, row=1)
# Bouton pour sélectionner le dossier d'entrée
ttk.Button(second_option_frame, text="Sélectionner un dossier", style='TButton',command=lambda: browse_folder(path_input_entry)).grid(column=2, row=1, pady=10)

ttk.Label(second_option_frame, text="Chemin de sortie:", font=('Helvetica', 14)).grid(column=0, row=2, sticky=tk.W, pady=10)
path_output_entry = ttk.Entry(second_option_frame)
path_output_entry.grid(column=1, row=2)
ttk.Button(second_option_frame, text="Sélectionner un dossier", style='TButton',command=lambda: browse_folder(path_output_entry)).grid(column=2, row=2, pady=10)

ttk.Label(second_option_frame, text="Fichier résultat Part 1:", font=('Helvetica', 14)).grid(column=0, row=3, sticky=tk.W, pady=10)
file_result_part1_entry = ttk.Entry(second_option_frame)
file_result_part1_entry.grid(column=1, row=3)

# Bouton pour sélectionner le fichier résultat Part 1
ttk.Button(second_option_frame, text="Sélectionner un fichier", command=lambda: browse_file_name(file_result_part1_entry)).grid(column=2, row=3, pady=10)

ttk.Label(second_option_frame, text="Nom du fichier de sortie:", font=('Helvetica', 14)).grid(column=0, row=4, sticky=tk.W, pady=10)
outputfile_name_entry = ttk.Entry(second_option_frame)
outputfile_name_entry.grid(column=1, row=4)
# Bouton pour appeler get_open_data
ttk.Button(second_option_frame, text="Démarrer l'extraction", style='TButton', command=get_open_data_interface).grid(column=0, row=5, columnspan=3, pady=20)

ttk.Button(second_option_frame, text="Retour à l'interface principale", style='TButton', command=show_main_interface).grid(column=0, row=6, columnspan=3, pady=20)

""" interface 3e option """
# Créer et ajouter des widgets à l'interface de la troisième option (dans le cadre de la troisième option)
ttk.Label(third_option_frame, text="Fusion", font=('Helvetica', 16)).grid(column=0, row=0, columnspan=3, pady=20)

# Entrées pour les fichiers de la troisième option
ttk.Label(third_option_frame, text="Fichier principal :", font=('Helvetica', 14)).grid(column=0, row=1, sticky=tk.W, pady=10)
main_file_entry_3 = ttk.Entry(third_option_frame)
main_file_entry_3.grid(column=1, row=1)
ttk.Button(third_option_frame, text="Sélectionner", style='TButton', command=lambda: browse_file_name(main_file_entry_3)).grid(column=2, row=1, pady=10)

ttk.Label(third_option_frame, text="Fichier secondaire :", font=('Helvetica', 14)).grid(column=0, row=2, sticky=tk.W, pady=10)
sub_file_entry_3 = ttk.Entry(third_option_frame)
sub_file_entry_3.grid(column=1, row=2)
ttk.Button(third_option_frame, text="Sélectionner", style='TButton', command=lambda: browse_file_name(sub_file_entry_3)).grid(column=2, row=2, pady=10)

ttk.Label(third_option_frame, text="Fichier de sortie :", font=('Helvetica', 14)).grid(column=0, row=3, sticky=tk.W, pady=10)
output_file_entry_3 = ttk.Entry(third_option_frame)
output_file_entry_3.grid(column=1, row=3)
ttk.Button(third_option_frame, text="Sélectionner", style='TButton', command=lambda: browse_file_name(output_file_entry_3)).grid(column=2, row=3, pady=10)

merge_button = ttk.Button(third_option_frame, text="Fusionner les fichiers", style='TButton', command=merge_csv)
merge_button.grid(column=0, row=4, columnspan=3, pady=10)

result_label = ttk.Label(third_option_frame, text="")
result_label.grid(column=0, row=5, columnspan=3)

# Bouton pour revenir à l'interface principale
ttk.Button(third_option_frame, text="Retour à l'interface principale", style='TButton', command=show_main_interface).grid(column=0, row=6, columnspan=3, pady=20)
# Affichez initialement l'interface principale

show_main_interface()

# Lancer l'interface
root.mainloop()

