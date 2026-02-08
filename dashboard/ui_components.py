import streamlit as st
import base64

def inject_atomic_css():
    st.markdown("""
    <head>
        <link rel="preconnect" href="https://fonts.googleapis.com">
        <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@300;400;500&family=Playfair+Display:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <style>
        /* --- RESET & BASICS --- */
        .stApp {
            background-color: #fdfcfb; /* Bone */
            color: #18181b; /* Obsidian */
            font-family: 'Inter', sans-serif;
        }
        
        /* Remove Streamlit default padding */
        .block-container {
            padding-top: 0 !important;
            padding-bottom: 5rem !important; /* Space for action bar */
            max-width: 800px !important;
        }
        
        /* Hide default header/footer */
        header {visibility: hidden;}
        footer {visibility: hidden;}
        
        /* --- TYPOGRAPHY --- */
        .font-serif-luxe {
            font-family: 'Playfair Display', serif;
            font-style: italic;
        }
        .font-mono-tactical {
            font-family: 'JetBrains Mono', monospace;
            letter-spacing: 0.05em;
        }
        
        /* --- COMPONENTS --- */
        
        /* Glass Header */
        .glass-header {
            background: rgba(253, 252, 251, 0.85);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border-bottom: 1px solid #efefec;
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            z-index: 50;
            height: 60px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        /* Panel Luxe */
        .panel-luxe {
            background: #ffffff;
            border: 1px solid #efefec;
            box-shadow: 0 1px 2px rgba(0,0,0,0.02);
            border-radius: 0px; /* Sharp tactical edges? User said "subtle shadows" but didn't specify radius. Let's go minimal radius. */
            border-radius: 4px;
            padding: 1.5rem;
            transition: all 0.2s ease;
        }
        .panel-luxe:hover {
            box-shadow: 0 4px 12px rgba(0,0,0,0.03);
            border-color: #e4e4e7;
        }
        
        /* Button: Obsidian Solid */
        .btn-obsidian {
            background-color: #18181b;
            color: #fff;
            font-family: 'Inter', sans-serif;
            font-weight: 500;
            padding: 0.5rem 1rem;
            border-radius: 4px;
            border: 1px solid #18181b;
            transition: all 0.2s;
        }
        .btn-obsidian:hover {
            background-color: #27272a;
            transform: translateY(-1px);
        }
        
        /* Button: Emergency Red Outline */
        .btn-emergency {
            background-color: transparent;
            color: #ef4444;
            border: 1px solid #ef4444;
            font-family: 'JetBrains Mono', monospace;
            padding: 0.5rem 1rem;
            border-radius: 4px;
        }
        .btn-emergency:hover {
            background-color: #fef2f2;
        }
        
        /* Global Grain */
        .grain-overlay {
            position: fixed;
            top: 0; 
            left: 0;
            width: 100%; 
            height: 100%;
            pointer-events: none;
            z-index: 9999;
            opacity: 0.03;
            background-image: url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noiseFilter'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.65' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noiseFilter)'/%3E%3C/svg%3E");
        }

        /* Shimmer Animation */
        @keyframes shimmer {
            0% {background-position: -1000px 0;}
            100% {background-position: 1000px 0;}
        }
        .animate-shimmer {
            animation: shimmer 2s infinite linear;
            background: linear-gradient(to right, #f4f4f5 4%, #e4e4e7 25%, #f4f4f5 36%);
            background-size: 1000px 100%;
        }
        
        /* Override Streamlit Buttons to be hidden so we can use custom HTML buttons triggered via Streamlit logic if needed, 
           OR we style Streamlit buttons to match. 
           Let's style Streamlit buttons to be minimal for now, but Action Bar will be HTML-ish if possible.
        */
        div.stButton > button {
           border-radius: 4px;
           border: 1px solid #e4e4e7;
           background: white;
           color: #18181b;
           font-family: 'JetBrains Mono', monospace;
           font-size: 12px;
        }
        div.stButton > button:hover {
           border-color: #18181b;
           color: #18181b;
        }
    </style>
    
    <div class="grain-overlay"></div>
    """, unsafe_allow_html=True)

def render_header(status_text="ONLINE", latency="24MS", title="BOT KERNEL"):
    """
    Renders the fixed sticky header.
    Note: We use st.markdown with fixed positioning.
    To avoid overlapping content, we added padding-top to block-container above.
    """
    # Header container
    st.markdown(f"""
    <div class="glass-header">
        <div class="max-w-[800px] w-full flex justify-between items-center px-4">
            <div class="flex items-center gap-2">
                <div class="w-2 h-2 bg-zinc-900 rounded-full"></div>
                <span class="font-mono-tactical text-xs font-bold tracking-widest text-zinc-900">{title}</span>
            </div>
            <div class="font-mono-tactical text-[10px] text-zinc-400">
                STATUS: <span class="text-zinc-800">{status_text}</span> • LATENCY: {latency}
            </div>
        </div>
    </div>
    <div style="height: 80px;"></div> <!-- Spacer -->
    """, unsafe_allow_html=True)

def render_page_header(title="BOT KERNEL"):
    """
    Helper to render header with auto-fetched status.
    """
    from dashboard.utils import get_bot_status
    status, _, _ = get_bot_status()
    
    status_text = "SYSTEM OPTIMAL" if status == "Running" else "SYSTEM IDLE"
    latency = "12MS" if status == "Running" else "OFFLINE"
    
    render_header(status_text=status_text, latency=latency, title=title)

def render_hero(status, bot_id):
    """
    The main Hero card showing Bot Activity.
    """
    # Logic for visual state
    is_active = status == "Running"
    status_color = "text-emerald-600" if is_active else "text-zinc-400"
    status_label = "ACTIVE" if is_active else "IDLE"
    
    st.markdown(f"""
    <div class="panel-luxe mb-6">
        <h2 class="font-serif-luxe text-3xl text-zinc-900 mb-2">Primary Objective</h2>
        <div class="font-mono-tactical text-xs text-zinc-400 mb-4 tracking-widest">SESSION ID: {bot_id if is_active else 'OFFLINE'}</div>
        
        <div class="flex items-center gap-4">
            <div class="flex-1">
                <p class="text-zinc-600 leading-relaxed font-light font-inter">
                    The bot is currently monitoring Telegram channels for culinary inquiries and rapid-response flavor profiling. 
                    {'Processing incoming streams.' if is_active else 'Systems are in standby mode.'}
                </p>
            </div>
            <div class="flex flex-col items-end">
                <span class="font-mono-tactical text-xs text-zinc-400">STATE</span>
                <span class="font-mono-tactical text-lg {status_color}">{status_label}</span>
            </div>
        </div>
        
        <!-- Processing Shimmer (only if active) -->
        {'<div class="h-[1px] w-full mt-6 animate-shimmer"></div>' if is_active else '<div class="h-[1px] w-full mt-6 bg-zinc-100"></div>'}
    </div>
    """, unsafe_allow_html=True)

def render_telemetry_grid(metrics):
    """
    Renders a 3-column grid of metrics.
    metrics: list of dicts {label, value, subtext}
    """
    cols = st.columns(3)
    
    for i, col in enumerate(cols):
        metric = metrics[i] if i < len(metrics) else None
        if metric:
            with col:
                st.markdown(f"""
                <div class="panel-luxe h-full flex flex-col justify-between">
                    <div class="font-mono-tactical text-[10px] text-zinc-400 tracking-widest uppercase mb-2">{metric['label']}</div>
                    <div class="font-mono-tactical text-xl text-zinc-900">{metric['value']}</div>
                    {f'<div class="text-[10px] text-zinc-400 mt-1 font-inter">{metric["subtext"]}</div>' if metric.get('subtext') else ''}
                </div>
                """, unsafe_allow_html=True)

def render_logs_viewer(logs):
    """
    Vertical list of logs styled cleanly.
    """
    st.markdown("""
    <div class="flex items-center justify-between mb-4 mt-8">
        <h3 class="font-serif-luxe text-xl text-zinc-900">System Logs</h3>
        <span class="font-mono-tactical text-xs text-zinc-400">LIVE FEED</span>
    </div>
    """, unsafe_allow_html=True)
    
    # We'll render the last 5 relevant logs in a styled HTML list
    log_lines = [l for l in logs.splitlines() if l.strip()]
    if not log_lines:
        Log_html = "<div class='text-zinc-400 font-mono-tactical text-sm'>No logs available.</div>"
    else:
        # Take last 8
        recent = log_lines[-8:]
        Log_rows = ""
        for line in recent:
            # Parse simple timestamp if possible (assuming format "DATE - NAME - LEVEL - MSG")
            parts = line.split(" - ")
            if len(parts) >= 4:
                ts = parts[0].split(" ")[1] # Just time
                level = parts[2]
                msg = parts[3]
            else:
                ts = "" 
                level = "INFO" 
                msg = line
            
            # Style based on level
            level_color = "text-red-500" if "ERROR" in level else "text-amber-500" if "WARNING" in level else "text-zinc-400"
            
            Log_rows += f"""
            <div class="flex gap-4 py-2 border-b border-zinc-100 items-start">
                <span class="font-mono-tactical text-[10px] text-zinc-300 w-16 shrink-0 pt-1">{ts}</span>
                <span class="font-mono-tactical text-[10px] {level_color} w-16 shrink-0 pt-1">{level}</span>
                <span class="font-inter text-xs text-zinc-600 leading-relaxed">{msg}</span>
            </div>
            """
        
        Log_html = f"<div class='flex flex-col'>{Log_rows}</div>"
    
    st.markdown(f"""
    <div class="panel-luxe min-h-[300px]">
        {Log_html}
    </div>
    """, unsafe_allow_html=True)

def render_action_bar_placeholder():
    """
    Since we can't easily do a fixed bottom bar with React logic in Streamlit that interacts with Python,
    we'll render a visually fixed bar but place the ACTUAL Streamlit buttons in a container at the bottom of the page
    or in the sidebar. 
    However, the request asks for a "Action Bar".
    
    Strategy: We'll put the buttons at the very bottom of the page content, styled to look grouped.
    Fixed position buttons in Streamlit (without custom component) are tricky because they don't trigger Python callbacks easily if they are pure HTML.
    
    Compromise: We render a "Control Panel" section at the bottom, styled distinctly.
    """
    st.markdown("---") 
    st.markdown('<h3 class="font-serif-luxe text-xl text-zinc-900 mb-4">Command & Control</h3>', unsafe_allow_html=True)
