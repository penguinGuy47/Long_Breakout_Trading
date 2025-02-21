from automate_buy import buy

def main():
    # Chrome profile paths
    chrome_path = r"C:\Users\kaile\AppData\Local\Google\Chrome\User Data"  # Update to your Chrome path
    chrome_profile = r"Profile 5"  # Change this as needed

    # ['item - color', 'size']
    items = [
        ['Baggy Jean - Washed Indigo', '30'],
        ['Duffle Bag - Snow Camo', 'One Size'],
    ]

    buy(items)  # Pass the list of item-size pairs

if __name__ == "__main__":
    main()
