# hf_batch_uploader.py

import os
import datetime
import zipfile
import json
from huggingface_hub import HfApi
from server import PromptServer

class HuggingFaceBatchUploader:
    """
    Um node ComfyUI que monitora pastas de imagens, cria um arquivo .zip em lotes
    e faz o upload para um repositório Hugging Face.
    """
    
    OUTPUT_NODE = True

    def __init__(self):
        pass

    @classmethod
    def INPUT_TYPES(s):
        """Define os tipos de input que o node aceita no frontend do ComfyUI."""
        return {
            "required": {
                "base_folder": ("STRING", {"multiline": False, "default": "D:/ComfyUI/output/MyCharacter"}),
                "hf_token": ("STRING", {"multiline": True, "default": ""}),
                "repo_id": ("STRING", {"multiline": False, "default": "username/repo-name"}),
                "upload_every_x_images": ("INT", {"default": 50, "min": 1, "max": 10000, "step": 1}),
                "seed": ("INT", {"default": 0, "min": 0, "max": 0xffffffffffffffff}),
            },
            "hidden": {"prompt": "PROMPT", "extra_pnginfo": "EXTRA_PNGINFO"},
            "optional": {
                "image_highquality": ("IMAGE",),
                "image_lowquality": ("IMAGE",),
                "image_watermarked": ("IMAGE",),
                "image_pixiv": ("IMAGE",),
            }
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("status",)
    FUNCTION = "execute"
    CATEGORY = "IO"

    def get_sorted_image_files(self, directory):
        """Retorna uma lista de arquivos de imagem (.png, .jpg, .jpeg, .webp) ordenados numericamente."""
        supported_extensions = {".png", ".jpg", ".jpeg", ".webp"}
        files = [f for f in os.listdir(directory) if os.path.splitext(f)[1].lower() in supported_extensions]
        files.sort(key=lambda f: int("".join(filter(str.isdigit, f)) or 0))
        return files

    def load_upload_log(self, log_path):
        """Carrega a lista de arquivos já enviados de um arquivo de log JSON."""
        if not os.path.exists(log_path):
            return set()
        try:
            with open(log_path, 'r') as f:
                return set(json.load(f))
        except (json.JSONDecodeError, IOError):
            return set()

    def save_upload_log(self, log_path, uploaded_files_set):
        """Salva a lista atualizada de arquivos enviados para o log JSON."""
        with open(log_path, 'w') as f:
            json.dump(list(uploaded_files_set), f, indent=4)

    def find_file_by_base_name(self, directory, base_name):
        """
        Encontra um arquivo em um diretório que corresponda a um nome base,
        ignorando a extensão E O CASE (maiúsculas/minúsculas).
        """
        supported_extensions = {".png", ".jpg", ".jpeg", ".webp"}
        base_name_lower = base_name.lower() # Converte o nome de referência para minúsculas uma vez
        for f in os.listdir(directory):
            file_base, file_ext = os.path.splitext(f)
            # *** AQUI ESTÁ A CORREÇÃO ***
            # Compara a versão em minúsculas de ambos os nomes base
            if file_base.lower() == base_name_lower and file_ext.lower() in supported_extensions:
                return f # Retorna o nome do arquivo original (com o case original)
        return None

    def execute(self, base_folder, hf_token, repo_id, upload_every_x_images, seed, prompt=None, extra_pnginfo=None, **kwargs):
        """Lógica principal do node."""
        if not hf_token or not repo_id or repo_id == "username/repo-name":
            return ("Token ou Repo ID do Hugging Face não fornecido. Pulando upload.",)

        if not os.path.isdir(base_folder):
            return (f"ERRO: A pasta base '{base_folder}' não existe.",)

        expected_subfolders = ["highquality", "lowquality", "watermarked", "pixiv"]
        subfolders = []
        for sub in expected_subfolders:
            subfolder_path = os.path.join(base_folder, sub)
            if os.path.isdir(subfolder_path):
                subfolders.append(sub)
            else:
                print(f"[HF Uploader] Aviso: Subpasta '{sub}' não encontrada, será ignorada.")
        
        if not subfolders:
            return (f"ERRO: Nenhuma das subpastas esperadas foi encontrada em '{base_folder}'.",)
        
        print(f"[HF Uploader] Pastas encontradas e que serão processadas: {subfolders}")

        log_file = os.path.join(base_folder, ".upload_log.json")
        uploaded_files_log = self.load_upload_log(log_file)
        
        hq_folder = os.path.join(base_folder, "highquality")
        
        all_hq_files = self.get_sorted_image_files(hq_folder)
        new_files_to_process = [f for f in all_hq_files if f not in uploaded_files_log]

        print(f"[HF Uploader] Status: {len(uploaded_files_log)} arquivos já upados. {len(new_files_to_process)} novos arquivos encontrados.")

        if len(new_files_to_process) < upload_every_x_images:
            status_msg = f"Aguardando... {len(new_files_to_process)}/{upload_every_x_images} novas imagens para o próximo lote."
            print(f"[HF Uploader] {status_msg}")
            return (status_msg,)

        reference_files_for_batch = new_files_to_process[:upload_every_x_images]
        
        base_folder_name = os.path.basename(os.path.normpath(base_folder))
        timestamp = datetime.datetime.now().strftime("%d-%m-%Y")
        zip_filename = f"{base_folder_name}_{timestamp}_{len(uploaded_files_log)}_to_{len(uploaded_files_log) + len(reference_files_for_batch)}.zip"
        zip_filepath = os.path.join(base_folder, zip_filename)

        print(f"[HF Uploader] Criando lote: {zip_filename} com {len(reference_files_for_batch)} imagens por pasta.")
        
        status_msg = ""
        try:
            base_names_for_batch = [os.path.splitext(f)[0] for f in reference_files_for_batch]

            with zipfile.ZipFile(zip_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                for subfolder_name in subfolders:
                    current_subfolder_path = os.path.join(base_folder, subfolder_name)
                    print(f"[HF Uploader] Processando pasta: {subfolder_name}")
                    
                    files_added_in_folder = 0
                    for base_name in base_names_for_batch:
                        actual_file_to_add = self.find_file_by_base_name(current_subfolder_path, base_name)
                        
                        if actual_file_to_add:
                            file_path = os.path.join(current_subfolder_path, actual_file_to_add)
                            arcname = os.path.join(subfolder_name, actual_file_to_add)
                            zf.write(file_path, arcname)
                            files_added_in_folder += 1
                        else:
                            print(f"[HF Uploader] Aviso: Arquivo com base '{base_name}' não encontrado em '{subfolder_name}'. Pulando.")
                    
                    print(f"[HF Uploader] {files_added_in_folder} arquivos adicionados da pasta {subfolder_name}")
            
            print(f"[HF Uploader] Fazendo upload de '{zip_filename}' para o repositório '{repo_id}'...")
            api = HfApi(token=hf_token)
            api.upload_file(
                path_or_fileobj=zip_filepath,
                path_in_repo=zip_filename,
                repo_id=repo_id,
                repo_type="model",
            )
            
            uploaded_files_log.update(reference_files_for_batch)
            self.save_upload_log(log_file, uploaded_files_log)
            
            status_msg = f"Sucesso! Upload de '{zip_filename}' concluído."
            print(f"[HF Uploader] {status_msg}")

        except Exception as e:
            status_msg = f"ERRO durante o processo de upload: {e}"
            print(f"[HF Uploader] {status_msg}")
            return (status_msg,)
        finally:
            if os.path.exists(zip_filepath) and status_msg.startswith("Sucesso"):
                os.remove(zip_filepath)
                print(f"[HF Uploader] Arquivo ZIP local '{zip_filename}' removido.")

        return (status_msg,)
