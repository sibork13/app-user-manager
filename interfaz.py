import streamlit as st
from PIL import Image, ImageDraw
import io
import base64

# Configuración de página
st.set_page_config(page_title="Página de Usuario", layout="wide")

# --- CSS PERSONALIZADO ---
st.markdown("""
    <style>
    /* 1. CONFIGURACIÓN DEL FONDO */
    .stApp {
        background-color: #E8EEF4; 
    }

    /* 2. ESTILO DE TARJETA CENTRAL */
    .main-card {
        background-color: #FFFFFF;
        padding: 2rem;
        padding-top: 1rem; 
        border-radius: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        border: 1px solid #E0E0E0;
        margin-top: 0rem;
    }

    /* 3. ESTILO DE BOTONES */
    .stButton > button {
        background-color: #3B82F6;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1rem;
        font-weight: 600;
        width: 100%;
        transition: background-color 0.3s ease;
    }
    
    .stButton > button:hover {
        background-color: #2563EB;
        border-color: #2563EB;
    }

    /* 4. TEXTOS DE AYUDA (CAPTIONS) */
    .custom-caption {
        color: #6B7280;
        font-size: 0.85rem;
        margin-top: -10px;
        margin-bottom: 15px;
        font-style: italic;
    }
    
    /* Clase para centrar textos */
    .centered-title {
        text-align: center;
        color: #1F2937;
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 0.5rem;
    }
    .centered-subtitle {
        text-align: center;
        color: #6B7280;
        margin-top: -15px;
        margin-bottom: 20px;
    }

    h3 {
        padding-top: 0 !important;
        margin-top: 0 !important;
    }
    </style>
""", unsafe_allow_html=True)

# --- FUNCIONES DE LOGO ---
def create_logo():
    img = Image.new('RGB', (200, 60), color = (59, 130, 246))
    d = ImageDraw.Draw(img)
    d.text((20, 20), "LOGO EMPRESA", fill=(255, 255, 255))
    return img

# --- INICIALIZACIÓN DE ESTADO ---
if 'page' not in st.session_state:
    st.session_state.page = 'main'

# --- INTERFAZ DE USUARIO ---

# Encabezado
col_logo, col_title, col_empty = st.columns([1, 2, 1])

with col_logo:
    logo = create_logo()
    st.image(logo, use_container_width=True)

with col_title:
    st.markdown('<div class="centered-title">Gestión de Usuario</div>', unsafe_allow_html=True)
    st.markdown('<div class="centered-subtitle">Panel de administración y control</div>', unsafe_allow_html=True)

# Contenedor Principal (Tarjeta Blanca)
col_spacer_l, col_content, col_spacer_r = st.columns([1, 2, 1])

with col_content:
    st.markdown('<div class="main-card">', unsafe_allow_html=True)

    # --- LÓGICA DE PÁGINAS ---
    
    # 1. PÁGINA PRINCIPAL
    if st.session_state.page == 'main':
        st.subheader("Selección de módulo")
        
        option = st.selectbox(
            'Seleccione una opción para continuar:',
            ["Seleccionar...", "Ver usuarios de grupos", "Agregar Usuarios de grupos", "Eliminar Usuarios de grupos"], # Lisado de opciones
            index=0
        )
        st.caption("ℹ️ Seleccione un módulo para acceder a las herramientas.")
        
        # Redirección según selección
        if option == "Ver usuarios de grupos":
            st.session_state.page = 'action_page' # Nueva página para la opción "uno"
            st.rerun()
        elif option == "Agregar Usuarios de grupos":
            st.session_state.page = 'group_page' # Nueva página para la opción "dos"
            st.rerun()

    # 2. PÁGINA DE ACCIÓN (OPCIÓN "UNO")
    elif st.session_state.page == 'action_page':
        st.subheader("Módulo de Acción")
        
        user_input = st.text_input("Ingrese los datos requeridos:")
        st.caption("📝 Escriba el identificador o nombre para procesar la solicitud.")
        
        st.write("---") 

        if st.button("Realizar acción principal"):
            st.session_state.show_message = True
        st.caption("⚡ Este botón ejecutará el proceso con los datos ingresados.")
            
        if st.session_state.get('show_message', False):
            st.success("✅ ¡Acción realizada correctamente!")
            
            if st.button("Regresar al inicio"):
                st.session_state.page = 'main'
                st.session_state.show_message = False
                st.rerun()
            st.caption("↩️ Vuelve al menú principal.")

    # 3. PÁGINA DE GRUPOS (OPCIÓN "DOS" - NUEVA FUNCIONALIDAD)
    elif st.session_state.page == 'group_page':
        st.subheader("Gestión de Grupos")
        
        # Dropdown solicitado
        group_option = st.selectbox(
            "Grupos disponibles",
            ["grupo 1", "grupo 2"] # Listado de grupos
        )
        st.caption("📂 Seleccione el grupo al que desea asignar o consultar.")
        
        # Textbox solicitado
        group_text = st.text_input("Comentarios o Descripción:")
        st.caption("📝 Ingrese detalles adicionales sobre el grupo seleccionado.")
        
        st.write("---")
        
        # Botón de regresar solicitado
        if st.button("Regresar al inicio"):
            st.session_state.page = 'main'
            st.rerun()
        st.caption("↩️ Vuelve al menú principal para seleccionar otra opción.")

    st.markdown('</div>', unsafe_allow_html=True)
