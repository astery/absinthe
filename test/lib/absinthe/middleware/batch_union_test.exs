defmodule Absinthe.Middleware.BatchUnionTest do
  use Absinthe.Case, async: true

  defmodule BatchCounter do
    def new() do
      Agent.start_link(fn -> 0 end)
    end

    def increment(pid) do
      Agent.update(pid, & &1 + 1)
    end

    def count(pid) do
      Agent.get(pid, & &1)
    end
  end

  defmodule Schema do
    use Absinthe.Schema

    @users [
      %{user_id: 1, organizations_ids: [1]},
      %{user_id: 2, organizations_ids: [1, 2]},
      %{user_id: 3, organizations_ids: [3, 4]},
    ]

    @organizations [
      %{organization_id: 1},
      %{organization_id: 2},
      %{organization_id: 3},
      %{organization_id: 4},
    ]

    object :error do
      field :message, :string
    end

    object :user do
      field :user_id, :integer
      field :organizations, list_of(:organization), resolve: &resolve_user_organization/3
    end

    object :organization do
      field :organization_id, :integer
    end

    object :users_list do
      field :items, list_of(:user)
    end

    union :get_user_result do
      types [:error, :users_list]
      resolve_type fn
        %{message: _}, _ -> :error
        _, _ -> :users_list
      end
    end

    query do
      field :get_users, :get_user_result do
        resolve fn
          _, _, _ -> {:ok, %{
            items: @users
          }}
        end
      end
    end

    defp resolve_user_organization(user, _args, info) do
      user_organization_ids = user.organizations_ids

      batch({__MODULE__, :user_organizations_batch_query, info}, user_organization_ids, fn batch_results ->
        {:ok, Enum.filter(batch_results, & &1.organization_id in user_organization_ids)}
      end)
    end

    def user_organizations_batch_query(%{context: %{batch_counter_pid: batch_counter_pid}}, users_organization_ids) do
      BatchCounter.increment(batch_counter_pid)

      ids = users_organization_ids |> List.flatten() |> Enum.uniq()
      Enum.filter(@organizations, & &1.organization_id in ids)
    end
  end

  it "should call batch function only once" do
    doc = """
    query getUsers {
      getUsers {
        __typename
        ... on UsersList {
          items {
            user_id
            organizations {
              organization_id
            }
          }
        }
      }
    }
    """
    {:ok, batch_counter_pid} = BatchCounter.new

    assert {:ok, %{data: data}} = Absinthe.run(doc, Schema, context: %{batch_counter_pid: batch_counter_pid})
    assert 1 == BatchCounter.count(batch_counter_pid)
  end
end
