import streamlit as st
import time

# Sayfa ayarları
st.set_page_config(
    page_title="Ana Sayfa",
    page_icon="🏠",
    layout="wide"
)

# Hata yakalama ve yeniden başlatma kontrolü
if 'app_restarted' not in st.session_state:
    st.session_state.app_restarted = False

# Eğer uygulama yeniden başlatıldıysa
if st.session_state.app_restarted:
    st.success("✅ Uygulama başarıyla yeniden başlatıldı!")
    st.session_state.app_restarted = False

# CSS stilleri
st.markdown("""
<style>
.main-header {
    text-align: center;
    color: #1f77b4;
    font-size: 3rem;
    margin-bottom: 2rem;
    font-weight: bold;
}

.user-card {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    border-radius: 15px;
    padding: 2rem;
    margin: 1rem 0;
    color: white;
    text-align: center;
    cursor: pointer;
    transition: all 0.3s ease;
    box-shadow: 0 8px 32px rgba(0,0,0,0.1);
}

.user-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 12px 40px rgba(0,0,0,0.2);
}

.user-name {
    font-size: 2rem;
    font-weight: bold;
    margin-bottom: 0.5rem;
}

.user-description {
    font-size: 1.2rem;
    opacity: 0.9;
}
</style>
""", unsafe_allow_html=True)

# Ana başlık
st.markdown('<h1 class="main-header">🏠 Ana Sayfa</h1>', unsafe_allow_html=True)

# Kullanıcı seçimi
st.markdown("### 🎯 Araç Seçimi")
col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    <div class="user-card">
        <div class="user-name">👨‍💻 Kerim</div>
        <div class="user-description">Excel Dönüştürme Aracı</div>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("Kerim'e Git", key="kerim_btn", use_container_width=True):
        st.switch_page("1_Kerim")

with col2:
    st.markdown("""
    <div class="user-card">
        <div class="user-name">👨‍💻 Caner</div>
        <div class="user-description">Bosch Aracı</div>
    </div>
    """, unsafe_allow_html=True)
    
    if st.button("Caner'e Git", key="caner_btn", use_container_width=True):
        st.switch_page("2_Caner") 