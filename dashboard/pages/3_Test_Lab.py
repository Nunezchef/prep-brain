import streamlit as st
import tempfile
import os
from dashboard.utils import load_config

# Import services
# We need to make sure we can import them.
# The dashboard runs from root, so `import services.brain` works.
try:
    from services.brain import chat
    from services.transcriber import transcribe_file
except ImportError:
    st.error("Could not import services. Make sure you are running from project root.")
    st.stop()

st.set_page_config(page_title="Test Lab", page_icon="🧪", layout="wide")
st.title("🧪 Test Lab")

col_brain, col_audio = st.columns(2)

with col_brain:
    st.header("🧠 Brain Test")
    st.markdown("Test the LLM response manually.")
    
    user_input = st.text_area("User Input")
    
    if st.button("Run Brain"):
        if not user_input:
            st.warning("Please enter some text.")
        else:
            with st.spinner("Thinking..."):
                # Construct a dummy history for one-shot test
                # Or use a selected session context? For simplicity, one-shot.
                history = [("user", user_input)]
                response = chat(history)
                st.write("### Response")
                st.info(response)

with col_audio:
    st.header("🎤 Audio Test")
    st.markdown("Test the transcriber pipeline.")
    
    audio_file = st.file_uploader("Upload Audio (OGG/WAV)", type=["ogg", "wav", "mp3"])
    
    if audio_file:
        if st.button("Transcribe"):
            with st.spinner("Transcribing..."):
                # Save temp file
                with tempfile.NamedTemporaryFile(delete=False, suffix=f".{audio_file.name.split('.')[-1]}") as tmp:
                    tmp.write(audio_file.getvalue())
                    tmp_path = tmp.name
                
                try:
                    text = transcribe_file(tmp_path)
                    st.success("Transcription Complete")
                    st.code(text)
                except Exception as e:
                    st.error(f"Error: {e}")
                finally:
                    os.unlink(tmp_path)
