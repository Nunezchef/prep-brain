import streamlit as st
import pandas as pd
from services import station_packs, recipes

st.set_page_config(page_title="Station Packs", page_icon="🗂️", layout="wide")

st.title("🗂️ Station Packs & Training Cards")

tab_station, tab_card = st.tabs(["📍 Station Packs", "🃏 Training Cards"])

# --- STATION PACK TAB ---
with tab_station:
    stations = station_packs.get_stations()
    
    if not stations:
        st.info("No stations found. Add stations to your recipes first.")
    else:
        selected_station = st.selectbox("Select Station", stations)
        
        if selected_station:
            pack = station_packs.get_station_pack(selected_station)
            
            st.markdown(f"## 📍 {pack['station']} Station")
            st.caption(f"{len(pack['recipes'])} recipes assigned")
            
            st.divider()
            
            for recipe in pack["recipes"]:
                with st.expander(f"**{recipe['name']}** — Yield: {recipe['yield_amount']} {recipe['yield_unit']} | Cost: ${recipe['total_cost']:,.2f}"):
                    c1, c2 = st.columns(2)
                    
                    with c1:
                        st.markdown("### Ingredients")
                        for ing in recipe["ingredients"]:
                            cost_display = f"(${ing['cost']:,.2f})" if ing.get('cost') else ""
                            st.text(f"• {ing['quantity']} {ing['unit']} {ing['display_name']} {cost_display}")
                    
                    with c2:
                        st.markdown("### Method")
                        st.write(recipe["method"] or "No method specified.")

# --- TRAINING CARD TAB ---
with tab_card:
    st.caption("Generate a printable training card for any recipe.")
    
    all_recipes = recipes.get_all_recipes()
    
    if not all_recipes:
        st.info("No recipes found.")
    else:
        selected_r = st.selectbox(
            "Select Recipe", 
            options=[r["id"] for r in all_recipes],
            format_func=lambda x: [r["name"] for r in all_recipes if r["id"]==x][0]
        )
        
        if selected_r:
            md = station_packs.generate_training_card_md(selected_r)
            
            st.markdown("---")
            st.markdown(md)
            
            st.download_button(
                "📥 Download Card (.md)",
                data=md,
                file_name=f"training_card_{selected_r}.md",
                mime="text/markdown"
            )
