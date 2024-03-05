import os
import subprocess
import time
import tkinter as tk
from tkinter import ttk
from ttkthemes import ThemedStyle
import getpass

# Get the current user's username
username = getpass.getuser()

# Function to get the script path
def get_script_path(script_name):
    local_path = fr'C:\Users\{username}\OneDrive - True Value Company\General - VendorOps & Experis\Rebate SQL Queries\Vendor_Detail_Tool_JZTest\Vendor_Detail_Scripts'
    script_path = os.path.join(local_path, script_name)
    print(f"Script path: {script_path}")
    return script_path

# Function to execute the selected script
def execute_script():
    execute_button.config(state="disabled")  # Disable the execute button during script execution
    progress["maximum"] = 100  # Set the maximum value for the progress bar

    def execute_script_thread():
        program_name = program_var.get()
        Vendor_number = vendor_entry.get()
        Year = year_entry.get()

        result_text.set("Please provide Vendor Information")  # Initialize result_text

        if program_name == "OSO":
            script_path = get_script_path("VendorOSODetailBackup.py")
        elif program_name == "Rebates":
            script_path = get_script_path("VendorRebatesDetail_Test.py")
        elif program_name == "MOL":
            script_path = get_script_path("VendorMOLDetailBackup.py")
        else:
            result_text.set("Invalid year or program name")
            execute_button.config(state="active")
            return

        result_text.set("Exporting Vendor Details")
        root.update_idletasks()

        for i in range(101):
            time.sleep(0.01)  # Simulated wait time (replace with actual script execution)
            progress["value"] = i
            root.update_idletasks()  # Update the GUI

        # Pass the user input to the script as arguments
        subprocess.Popen(["python", script_path, Vendor_number, Year])

        progress["value"] = 0  # Reset the progress bar after the script finishes

        result_text.set("Please Enter Vendor Information")
        execute_button.config(state="active")  # Re-enable the execute button

    # Create a separate thread for script execution
    import threading
    script_thread = threading.Thread(target=execute_script_thread)
    script_thread.start()

# Function to execute the script when Enter key is pressed
def on_enter_key(event):
    execute_script()

# Create the main window
root = tk.Tk()
root.title("True Value Vendor Detail Tool")

style = ThemedStyle(root)
style.set_theme("plastik")

# Add a header label with True Value's color scheme
header_label = ttk.Label(root, text="True Value Vendor Detail Tool", font=("Comic Sans MS", 14, "bold italic"), background="black", foreground="red")
header_label.pack(pady=10)

# Set background color for the main window
root.configure(bg="black")

# Create a frame with True Value's colors
frame = ttk.Frame(root, padding=20, style="My.TFrame")
frame.pack(fill=tk.BOTH, expand=True)

# Create and configure a label for Vendor Number
vendor_label = ttk.Label(frame, text="Enter Vendor Number:", style="TV.TLabel")
vendor_label.grid(row=0, column=0, padx=(0, 5), pady=(0, 5))
vendor_entry = ttk.Entry(frame)
vendor_entry.grid(row=0, column=1, padx=5, pady=(0, 5))

# Create and configure a label for Rebate Year
year_label = ttk.Label(frame, text="Enter Rebate Year:", style="TV.TLabel")
year_label.grid(row=1, column=0, padx=(0, 5), pady=5)
year_entry = ttk.Entry(frame)
year_entry.grid(row=1, column=1, padx=5, pady=5)

# Create a label and combobox for Program Name with TV's colors
program_label = ttk.Label(frame, text="Select Program Name:", style="TV.TLabel")
program_label.grid(row=2, column=0, padx=(0, 5), pady=5)
program_var = tk.StringVar()
program_combo = ttk.Combobox(frame, textvariable=program_var, values=["OSO", "Rebates","MOL"])
program_combo.grid(row=2, column=1, padx=5, pady=5)

# Create a button with True Value's colors
execute_button = ttk.Button(frame, text="Execute", command=execute_script, style="TV.TButton")
execute_button.grid(row=3, columnspan=2, pady=(10, 0))

# Create a progress bar with True Value's colors
progress = ttk.Progressbar(frame, orient="horizontal", length=300, mode="determinate", style="TV.Horizontal.TProgressbar")
progress.grid(row=4, columnspan=2, pady=10)

# Initialize result_text as a StringVar with True Value's colors
result_text = tk.StringVar()
result_text.set("Please Enter Vendor Information")
result_label = ttk.Label(frame, textvariable=result_text, style="TV.TLabel", background="white")
result_label.grid(row=5, columnspan=2, pady=5)

# Configure the style for True Value's colors
style.configure("My.TFrame", background='lightgray')
style.configure("TV.TLabel", background="lightgray", foreground="black")
style.configure("TV.TButton", background="red", foreground="black")
style.configure("TV.Horizontal.TProgressbar", troughcolor="red", background="white")
style.map("TV.Horizontal.TProgressbar", background=[("active", "red")])

# Bind the Enter key to the main window
root.bind("<Return>", on_enter_key)

# Start the main loop
root.mainloop()
