# Contact Book Application

def add_con(contacts, name, phone):
    # To check if the contact already exists
    for contact in contacts:
        if contact[0].lower() == name.lower():
            print(f"Contact with name '{name}' already exists.")
            return
    # Add new contact
    contacts.append((name, phone))
    print(f"Contact '{name}' added successfully.")


def search_con(contacts, name):
    # Search for the contact by name
    for contact in contacts:
        if contact[0].lower() == name.lower():
            print(f"Found Contact: Name: {contact[0]}, Phone: {contact[1]}.")
            return
    print(f"Contact with name '{name}' does not exist.")


def display_con(contacts):
    # Display all contacts
    if not contacts:
        print("No contacts to display.")
    else:
        print("Contact List:")
        for contact in contacts:
            print(f"Name: {contact[0]}, Phone: {contact[1]}")


def main():
    contacts = []
    while True:
        print("\nContact Book Menu:")
        print("1. Add Contact")
        print("2. Search Contact")
        print("3. Display Contacts")
        print("4. Quit")

        choice = input("Enter your choice: ")
        if choice == "1":
            name = input("Enter Contact Name: ")
            phone = input("Enter Contact Phone: ")
            add_con(contacts, name, phone)
        elif choice == "2":
            name = input("Enter Contact Name: ")
            search_con(contacts, name)
        elif choice == "3":
            display_con(contacts)
        elif choice == "4":
            print("Exiting Contact Book!")
            break
        else:
            print("Invalid choice. Please try again.")

        # Ask if the user wants to continue
        cont = input("Do you want to continue? (y/n): ").lower()
        if cont != 'y':
            print("Exiting Contact Book!")
            break


if __name__ == "__main__":
    main()
